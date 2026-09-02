package aliyunpan

import (
	"testing"
	"time"
)

func TestOSSURLExpiryReadsBothSignatureVersions(t *testing.T) {
	// v4 signs when the link was issued and how long it lives.
	v4 := "https://cdn.aliyundrive.net/file.bin?x-oss-date=20260101T000000Z&x-oss-expires=14400&x-oss-signature=abc"
	deadline, ok := ossURLExpiry(v4)
	if !ok {
		t.Fatal("a v4 signed link was not understood")
	}
	want := time.Date(2026, 1, 1, 4, 0, 0, 0, time.UTC)
	if !deadline.Equal(want) {
		t.Errorf("v4 deadline = %s, want %s", deadline, want)
	}

	// v2 and v3 sign the deadline itself.
	v2 := "https://cdn.aliyundrive.net/file.bin?Expires=1767225600&OSSAccessKeyId=key&Signature=abc"
	deadline, ok = ossURLExpiry(v2)
	if !ok {
		t.Fatal("a v2 signed link was not understood")
	}
	if !deadline.Equal(time.Unix(1767225600, 0)) {
		t.Errorf("v2 deadline = %s, want %s", deadline, time.Unix(1767225600, 0))
	}
}

// A link whose deadline cannot be read has to fall through to the reactive
// path. Guessing "expired" would make every such link be refreshed before every
// single chunk.
func TestOSSURLExpiryRejectsUnreadableLinks(t *testing.T) {
	for name, raw := range map[string]string{
		"no query":            "https://cdn.aliyundrive.net/file.bin",
		"no expiry":           "https://cdn.aliyundrive.net/file.bin?x-oss-signature=abc",
		"expiry not a number": "https://cdn.aliyundrive.net/file.bin?x-oss-expires=soon",
		"negative expiry":     "https://cdn.aliyundrive.net/file.bin?x-oss-expires=-1",
		"bad issue time":      "https://cdn.aliyundrive.net/file.bin?x-oss-date=yesterday&x-oss-expires=100",
		// A lifetime with no start time says nothing about when the link dies,
		// and reading it as a deadline would place it in 1970.
		"lifetime without start": "https://cdn.aliyundrive.net/file.bin?x-oss-expires=14400",
	} {
		if _, ok := ossURLExpiry(raw); ok {
			t.Errorf("%s: expected the link's deadline to be unreadable", name)
		}
		if downloadURLExhausted(raw, time.Now()) {
			t.Errorf("%s: an unreadable deadline must not be treated as expired", name)
		}
	}
}

// The grace period is what stops a link from dying halfway through a chunk,
// which wastes the whole range rather than just the request.
func TestDownloadURLExhaustedAppliesGracePeriod(t *testing.T) {
	issued := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	raw := "https://cdn.aliyundrive.net/file.bin?x-oss-date=20260101T000000Z&x-oss-expires=14400"
	deadline := issued.Add(4 * time.Hour)

	if downloadURLExhausted(raw, issued) {
		t.Error("a freshly issued link was treated as spent")
	}
	if downloadURLExhausted(raw, deadline.Add(-downloadURLGrace-time.Second)) {
		t.Error("a link with more than the grace period left was treated as spent")
	}
	if !downloadURLExhausted(raw, deadline.Add(-downloadURLGrace+time.Second)) {
		t.Error("a link inside the grace period should be replaced before it is used")
	}
	if !downloadURLExhausted(raw, deadline.Add(time.Second)) {
		t.Error("an expired link was treated as usable")
	}
}
