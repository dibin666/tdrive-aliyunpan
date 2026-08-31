package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/rpc"
	"sync"

	goPlugin "github.com/hashicorp/go-plugin"
)

// PluginName is the stable name used in the go-plugin plugin map.
const PluginName = "tdrive"

const (
	magicCookieKey   = "TDRIVE_PLUGIN_RPC"
	magicCookieValue = "tdrive-plugin-v1"
)

// HandshakeConfig is shared by plugin binaries and the tdrive host.
var HandshakeConfig = goPlugin.HandshakeConfig{
	ProtocolVersion:  ProtocolVersion,
	MagicCookieKey:   magicCookieKey,
	MagicCookieValue: magicCookieValue,
}

// RPCPlugin adapts the public Plugin interface to go-plugin's net/rpc
// transport. It is used by both Serve and the host's ClientConfig.
type RPCPlugin struct {
	Impl Plugin
}

func (p *RPCPlugin) Server(broker *goPlugin.MuxBroker) (interface{}, error) {
	if p.Impl == nil {
		return nil, errors.New("tdrive plugin implementation is nil")
	}
	return &RPCServer{Impl: p.Impl, broker: broker}, nil
}

func (p *RPCPlugin) Client(broker *goPlugin.MuxBroker, client *rpc.Client) (interface{}, error) {
	return &Client{rpc: client, broker: broker}, nil
}

// Serve starts the go-plugin server and blocks until the host disconnects.
func Serve(implementation Plugin) {
	goPlugin.Serve(&goPlugin.ServeConfig{
		HandshakeConfig: HandshakeConfig,
		Plugins: goPlugin.PluginSet{
			PluginName: &RPCPlugin{Impl: implementation},
		},
	})
}

// Client is the host-side implementation returned by go-plugin Dispense.
// AttachHost must be called before the plugin can initialize. The host service
// is exposed over a second brokered connection, which gives plugins a complete
// bidirectional SDK without sharing the host process memory.
type Client struct {
	rpc    *rpc.Client
	broker *goPlugin.MuxBroker

	mu           sync.Mutex
	hostAttached bool
	hostBrokerID uint32
}

// Manifest returns the manifest reported by the compiled plugin binary.
func (client *Client) Manifest(ctx context.Context) (Manifest, error) {
	var response Manifest
	if err := client.call(ctx, "Plugin.Manifest", struct{}{}, &response); err != nil {
		return Manifest{}, err
	}
	return response, nil
}

// AttachHost starts the reverse RPC service and initializes the plugin.
func (client *Client) AttachHost(ctx context.Context, host Host) error {
	if host == nil {
		return errors.New("plugin host is nil")
	}

	client.mu.Lock()
	if client.hostAttached {
		client.mu.Unlock()
		return errors.New("plugin host is already attached")
	}
	brokerID := client.broker.NextId()
	client.hostBrokerID = brokerID
	client.hostAttached = true
	client.mu.Unlock()

	go client.broker.AcceptAndServe(brokerID, &HostRPCServer{Host: host, broker: client.broker})
	var response struct{}
	if err := client.call(ctx, "Plugin.Initialize", InitializeArgs{
		HostBrokerID:      brokerID,
		DeadlineUnixMilli: deadlineUnixMilli(ctx),
	}, &response); err != nil {
		client.mu.Lock()
		client.hostAttached = false
		client.mu.Unlock()
		return err
	}
	return nil
}

// Before sends an operation to the optional plugin hook.
func (client *Client) Before(ctx context.Context, operation Operation) (OperationResult, error) {
	var response OperationResult
	operation.DeadlineUnixMilli = deadlineUnixMilli(ctx)
	if err := client.call(ctx, "Plugin.Before", operation, &response); err != nil {
		return OperationResult{}, err
	}
	return response, nil
}

// After sends an operation to the optional post-operation hook.
func (client *Client) After(ctx context.Context, operation Operation) error {
	var response struct{}
	operation.DeadlineUnixMilli = deadlineUnixMilli(ctx)
	return client.call(ctx, "Plugin.After", operation, &response)
}

// OnEvent delivers one event to the plugin.
func (client *Client) OnEvent(ctx context.Context, event Event) error {
	var response struct{}
	event.DeadlineUnixMilli = deadlineUnixMilli(ctx)
	return client.call(ctx, "Plugin.OnEvent", event, &response)
}

// HandleHTTP calls a plugin-owned HTTP route.
func (client *Client) HandleHTTP(ctx context.Context, request HTTPRequest) (HTTPResponse, error) {
	var response HTTPResponse
	request.DeadlineUnixMilli = deadlineUnixMilli(ctx)
	if err := client.call(ctx, "Plugin.HandleHTTP", request, &response); err != nil {
		return HTTPResponse{}, err
	}
	return response, nil
}

// Shutdown gives the plugin a chance to flush its own state before the host
// kills the child process.
func (client *Client) Shutdown(ctx context.Context) error {
	var response struct{}
	return client.call(ctx, "Plugin.Shutdown", ShutdownArgs{DeadlineUnixMilli: deadlineUnixMilli(ctx)}, &response)
}

func (client *Client) call(ctx context.Context, method string, args, response any) error {
	if ctx == nil {
		ctx = context.Background()
	}
	call := client.rpc.Go(method, args, response, make(chan *rpc.Call, 1))
	select {
	case result := <-call.Done:
		return result.Error
	case <-ctx.Done():
		return ctx.Err()
	}
}

// InitializeArgs contains the broker ID the plugin uses to reach the host.
type InitializeArgs struct {
	HostBrokerID      uint32
	DeadlineUnixMilli int64
}

type ShutdownArgs struct {
	DeadlineUnixMilli int64
}

// RPCServer is the plugin-side net/rpc implementation.
type RPCServer struct {
	Impl   Plugin
	broker *goPlugin.MuxBroker

	mu       sync.Mutex
	host     *HostClient
	hostConn io.ReadWriteCloser
}

func (server *RPCServer) Manifest(_ struct{}, response *Manifest) error {
	manifest := server.Impl.Manifest()
	if err := manifest.Validate(); err != nil {
		return err
	}
	*response = manifest
	return nil
}

func (server *RPCServer) Initialize(args InitializeArgs, _ *struct{}) error {
	server.mu.Lock()
	if server.host != nil {
		server.mu.Unlock()
		return errors.New("plugin is already initialized")
	}
	server.mu.Unlock()

	connection, err := server.broker.Dial(args.HostBrokerID)
	if err != nil {
		return fmt.Errorf("connect to tdrive host: %w", err)
	}
	hostClient := &HostClient{rpc: rpc.NewClient(connection), broker: server.broker}
	callCtx, cancel := contextWithDeadline(args.DeadlineUnixMilli)
	defer cancel()
	if err := server.Impl.Initialize(callCtx, hostClient); err != nil {
		_ = connection.Close()
		return err
	}

	server.mu.Lock()
	server.host = hostClient
	server.hostConn = connection
	server.mu.Unlock()
	return nil
}

func (server *RPCServer) Before(operation Operation, response *OperationResult) error {
	hook, ok := server.Impl.(BeforeHook)
	if !ok {
		*response = OperationResult{Allowed: true, Payload: operation.Payload}
		return nil
	}
	callCtx, cancel := contextWithDeadline(operation.DeadlineUnixMilli)
	defer cancel()
	result, err := hook.Before(callCtx, operation)
	if err != nil {
		return err
	}
	*response = result
	return nil
}

func (server *RPCServer) After(operation Operation, _ *struct{}) error {
	if hook, ok := server.Impl.(AfterHook); ok {
		callCtx, cancel := contextWithDeadline(operation.DeadlineUnixMilli)
		defer cancel()
		hook.After(callCtx, operation)
	}
	return nil
}

func (server *RPCServer) OnEvent(event Event, _ *struct{}) error {
	if hook, ok := server.Impl.(EventHook); ok {
		callCtx, cancel := contextWithDeadline(event.DeadlineUnixMilli)
		defer cancel()
		hook.OnEvent(callCtx, event)
	}
	return nil
}

func (server *RPCServer) HandleHTTP(request HTTPRequest, response *HTTPResponse) error {
	hook, ok := server.Impl.(HTTPHook)
	if !ok {
		return errors.New("plugin does not implement HTTP routes")
	}
	callCtx, cancel := contextWithDeadline(request.DeadlineUnixMilli)
	defer cancel()
	result, err := hook.HandleHTTP(callCtx, request)
	if err != nil {
		return err
	}
	if result.Status == 0 {
		result.Status = 200
	}
	*response = result
	return nil
}

func (server *RPCServer) Shutdown(args ShutdownArgs, _ *struct{}) error {
	var shutdownErr error
	if hook, ok := server.Impl.(ShutdownHook); ok {
		callCtx, cancel := contextWithDeadline(args.DeadlineUnixMilli)
		defer cancel()
		shutdownErr = hook.Shutdown(callCtx)
	}
	server.mu.Lock()
	if server.hostConn != nil {
		_ = server.hostConn.Close()
	}
	server.hostConn = nil
	server.host = nil
	server.mu.Unlock()
	return shutdownErr
}

// HostClient is the public SDK implementation used inside a plugin process.
type HostClient struct {
	rpc    *rpc.Client
	broker *goPlugin.MuxBroker
}

func (client *HostClient) Call(ctx context.Context, method string, request any, response any) error {
	payload, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("encode host request: %w", err)
	}
	var result HostCallResponse
	if err := client.call(ctx, "Plugin.Call", HostCallRequest{
		Method:            method,
		Payload:           payload,
		DeadlineUnixMilli: deadlineUnixMilli(ctx),
	}, &result); err != nil {
		return err
	}
	if result.Error != "" {
		return errors.New(result.Error)
	}
	if response == nil || len(result.Payload) == 0 {
		return nil
	}
	if err := json.Unmarshal(result.Payload, response); err != nil {
		return fmt.Errorf("decode host response: %w", err)
	}
	return nil
}

func (client *HostClient) OpenStream(ctx context.Context, method string, request any) (io.ReadWriteCloser, error) {
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("encode host stream request: %w", err)
	}
	brokerID := client.broker.NextId()
	var response struct{}
	if err := client.call(ctx, "Plugin.OpenStream", HostStreamRequest{
		BrokerID:          brokerID,
		Method:            method,
		Payload:           payload,
		DeadlineUnixMilli: deadlineUnixMilli(ctx),
	}, &response); err != nil {
		return nil, err
	}
	connection, err := client.broker.Dial(brokerID)
	if err != nil {
		return nil, fmt.Errorf("connect host stream: %w", err)
	}
	return connection, nil
}

func (client *HostClient) call(ctx context.Context, method string, args, response any) error {
	if ctx == nil {
		ctx = context.Background()
	}
	call := client.rpc.Go(method, args, response, make(chan *rpc.Call, 1))
	select {
	case result := <-call.Done:
		return result.Error
	case <-ctx.Done():
		return ctx.Err()
	}
}

// HostCallRequest and HostCallResponse are the raw reverse-RPC envelope.
type HostCallRequest struct {
	Method            string
	Payload           []byte
	DeadlineUnixMilli int64
}

type HostCallResponse struct {
	Payload []byte
	Error   string
}

// HostStreamRequest asks the host to broker an io.ReadWriteCloser.
type HostStreamRequest struct {
	BrokerID          uint32
	Method            string
	Payload           []byte
	DeadlineUnixMilli int64
}

// HostRPCServer runs in the tdrive process and forwards calls to the concrete
// host implementation supplied by internal/plugin.
type HostRPCServer struct {
	Host   Host
	broker *goPlugin.MuxBroker
}

func (server *HostRPCServer) Call(request HostCallRequest, response *HostCallResponse) error {
	callCtx, cancel := contextWithDeadline(request.DeadlineUnixMilli)
	defer cancel()
	var payload json.RawMessage
	if err := server.Host.Call(callCtx, request.Method, json.RawMessage(request.Payload), &payload); err != nil {
		response.Error = err.Error()
		return nil
	}
	response.Payload = payload
	return nil
}

func (server *HostRPCServer) OpenStream(request HostStreamRequest, _ *struct{}) error {
	callCtx, cancel := contextWithDeadline(request.DeadlineUnixMilli)
	go func() {
		defer cancel()
		connection, err := server.broker.Accept(request.BrokerID)
		if err != nil {
			return
		}
		defer connection.Close()

		stream, err := server.Host.OpenStream(
			callCtx,
			request.Method,
			json.RawMessage(request.Payload),
		)
		if err != nil {
			return
		}
		defer stream.Close()
		bridgeStreams(connection, stream)
	}()
	return nil
}

func bridgeStreams(left io.ReadWriteCloser, right io.ReadWriteCloser) {
	copyDone := make(chan struct{}, 2)
	go func() {
		_, _ = io.Copy(right, left)
		if closer, ok := right.(interface{ CloseWrite() error }); ok {
			_ = closer.CloseWrite()
		}
		copyDone <- struct{}{}
	}()
	go func() {
		_, _ = io.Copy(left, right)
		copyDone <- struct{}{}
	}()
	<-copyDone
	_ = left.Close()
	_ = right.Close()
	<-copyDone
}
