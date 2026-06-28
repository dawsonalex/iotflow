package http

import (
	"encoding/json"
	"io"
	"net/http"
	"time"
)

// doSseResponse listens to tChan and writes JSON marshaled values to rw in SSE format, flushing rw
// after each event. If an error occurs marshaling a value, it is passed to errHandler (errHandler
// may be nil) and the loop stops.
//
// When keepalive is positive, a heartbeat comment is written every keepalive interval while no
// values arrive; this keeps idle connections alive through proxies and surfaces a dead client (a
// failed write returns) without waiting on r.Context(). A non-positive keepalive disables it.
//
// The loop stops when r.Context() is cancelled (client disconnect or server shutdown), when tChan
// closes, or when a write to rw fails.
func doSseResponse[T any](rw http.ResponseWriter, r *http.Request, tChan <-chan T, keepalive time.Duration, errHandler func(r *http.Request, err error)) {
	flusher, ok := rw.(http.Flusher)
	if !ok {
		http.Error(rw, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	rw.Header().Set("Content-Type", "text/event-stream")
	rw.Header().Set("Cache-Control", "no-cache")

	// A nil channel never fires, which disables heartbeats for a non-positive interval.
	var beat <-chan time.Time
	if keepalive > 0 {
		ticker := time.NewTicker(keepalive)
		defer ticker.Stop()
		beat = ticker.C
	}

	for {
		select {
		case <-r.Context().Done():
			return
		case <-beat:
			if _, err := io.WriteString(rw, ": keepalive\n\n"); err != nil {
				return // client gone
			}
			flusher.Flush()
		case t, ok := <-tChan:
			if !ok {
				return
			}

			payload, err := json.Marshal(t)
			if err != nil {
				if errHandler != nil {
					errHandler(r, err)
				}
				return
			}
			if _, err := io.WriteString(rw, "data: "+string(payload)+"\n\n"); err != nil {
				return // client gone
			}
			flusher.Flush()
		}
	}
}
