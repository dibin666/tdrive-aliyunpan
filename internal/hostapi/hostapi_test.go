package hostapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
)

type stubHost struct {
	err   error
	value json.RawMessage
}

func (h stubHost) Call(_ context.Context, _ string, _ any, response any) error {
	if h.err != nil {
		return h.err
	}
	if target, ok := response.(*json.RawMessage); ok {
		*target = h.value
	}
	return nil
}

func (stubHost) OpenStream(context.Context, string, any) (io.ReadWriteCloser, error) {
	return nil, nil
}

// The host answers data.get for an unwritten key with a not-found error rather
// than a null. Reading that as a failure would make the first start of a fresh
// installation look broken, so it has to be treated as "no value yet".
func TestGetDataTreatsMissingKeyAsAbsent(t *testing.T) {
	client := New(stubHost{err: errors.New("database: not found")})
	var target map[string]any
	found, err := client.GetData(context.Background(), "settings", &target)
	if err != nil {
		t.Fatalf("GetData: %v", err)
	}
	if found {
		t.Error("a missing key should report found = false")
	}
}

func TestGetDataPropagatesRealErrors(t *testing.T) {
	client := New(stubHost{err: errors.New("database is locked")})
	var target map[string]any
	if _, err := client.GetData(context.Background(), "settings", &target); err == nil {
		t.Fatal("a genuine failure must not be swallowed")
	}
}

func TestGetDataDecodes(t *testing.T) {
	client := New(stubHost{value: json.RawMessage(`{"a":1}`)})
	var target map[string]int
	found, err := client.GetData(context.Background(), "settings", &target)
	if err != nil || !found {
		t.Fatalf("GetData: found = %v, err = %v", found, err)
	}
	if target["a"] != 1 {
		t.Errorf("decoded %v", target)
	}
}

// The drive and the database word their not-found errors differently, and
// both reach the plugin as plain strings.
func TestIsNotFound(t *testing.T) {
	cases := map[string]bool{
		"database: not found":                        true,
		"drive: no such file or directory":           true,
		"drive: no such file or directory: /阿里云盘/影视": true,
		"database is locked":                         false,
		"telegram: FLOOD_WAIT_30":                    false,
	}
	for message, want := range cases {
		if got := IsNotFound(errors.New(message)); got != want {
			t.Errorf("IsNotFound(%q) = %v, want %v", message, got, want)
		}
	}
	if IsNotFound(nil) {
		t.Error("IsNotFound(nil) should be false")
	}
}

func TestGetDataHandlesNull(t *testing.T) {
	client := New(stubHost{value: json.RawMessage(`null`)})
	var target map[string]any
	found, err := client.GetData(context.Background(), "settings", &target)
	if err != nil {
		t.Fatalf("GetData: %v", err)
	}
	if found {
		t.Error("a null value should report found = false")
	}
}
