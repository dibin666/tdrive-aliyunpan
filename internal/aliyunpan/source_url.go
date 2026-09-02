package aliyunpan

import (
	"net/url"
	"strconv"
	"strings"
	"time"
)

// downloadURLGrace is how much of a signed link's life is treated as already
// spent. A chunk takes a while to arrive, and a link that expires halfway
// through the response wastes the whole range rather than just the request.
const downloadURLGrace = 60 * time.Second

// ossExpiryLayout is the ISO 8601 basic form OSS signs its start time with.
const ossExpiryLayout = "20060102T150405Z"

// earliestAbsoluteExpiry rules out a relative lifetime that was mistaken for a
// deadline. It is 2001-09-09, far enough in the past to accept every real
// signature and far enough in the future that no plausible "seconds from now"
// value reaches it.
const earliestAbsoluteExpiry = int64(1_000_000_000)

// ossURLExpiry reads when a signed download link stops working.
//
// The drive is asked for four hours (getDownloadURL's ExpireSec), while a
// single file may be downloaded for up to twelve, so a large file will outlive
// its link. Waiting for the 403 that proves it costs one wasted request and one
// retry attempt per chunk in flight; the deadline is right there in the query
// string, so it is read instead.
//
// OSS v4 signs a start time in x-oss-date and a lifetime in x-oss-expires,
// while v2 and v3 sign an absolute deadline in x-oss-expires or Expires. A link
// in neither shape returns false, and the caller falls back to reacting to the
// rejection.
func ossURLExpiry(raw string) (time.Time, bool) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return time.Time{}, false
	}
	query := parsed.Query()
	expires := strings.TrimSpace(query.Get("x-oss-expires"))
	if expires == "" {
		expires = strings.TrimSpace(query.Get("Expires"))
	}
	if expires == "" {
		return time.Time{}, false
	}
	seconds, err := strconv.ParseInt(expires, 10, 64)
	if err != nil || seconds <= 0 {
		return time.Time{}, false
	}
	if issued := strings.TrimSpace(query.Get("x-oss-date")); issued != "" {
		start, parseErr := time.Parse(ossExpiryLayout, issued)
		if parseErr != nil {
			return time.Time{}, false
		}
		return start.Add(time.Duration(seconds) * time.Second), true
	}
	if seconds < earliestAbsoluteExpiry {
		// A lifetime without the start time it is relative to says nothing
		// about when the link dies, and reading it as a deadline would declare
		// every such link expired since 1970.
		return time.Time{}, false
	}
	return time.Unix(seconds, 0), true
}

// downloadURLExhausted reports whether a link is too close to its deadline to
// be worth using. A link whose deadline cannot be read is always worth trying:
// the reactive path still catches it.
func downloadURLExhausted(raw string, now time.Time) bool {
	deadline, ok := ossURLExpiry(raw)
	if !ok {
		return false
	}
	return !deadline.After(now.Add(downloadURLGrace))
}
