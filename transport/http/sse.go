package http

import (
	"encoding/json"
	"net/http"
)

// doSseResponse listens to tChan and writes JSON marshaled values to rw in SSE format, flushing rw
// after each event. If an error occurs, it is passed to errHandler (errHandler may be nil).
// r.Context() is used to cancel the channel loop.
func doSseResponse[T any](rw http.ResponseWriter, r *http.Request, tChan <-chan T, errHandler func(r *http.Request, err error)) {
	rw.Header().Set("Content-Type", "text/event-stream")
	rw.Header().Set("Cache-Control", "no-cache")

	// TODO: This only sends future events. For clients that sub after a flow starts, we should track either the
	// last or all previous state evnets so the client knows where they're at.
	for {
		select {
		case <-r.Context().Done():
			return
		case t, ok := <-tChan:
			if !ok {
				return
			}

			payload, err := json.Marshal(t)
			if err != nil && errHandler != nil {
				errHandler(r, err)
				return
			}
			_, _ = rw.Write([]byte("data: " + string(payload) + "\n\n"))

			flusher, ok := rw.(http.Flusher)
			if !ok {
				http.Error(rw, "streaming unsupported", http.StatusInternalServerError)
				return
			}
			flusher.Flush()
		}
	}
}
