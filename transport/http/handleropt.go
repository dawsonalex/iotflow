package http

import (
	"net"
	"net/http"
	"time"
)

type HandlerOpt func(h *handler)

func WithErrorHandler(f func(r *http.Request, err error)) HandlerOpt {
	return func(h *handler) { h.onError = f }
}

// WithKeepalive sets how often an idle SSE stream emits a heartbeat comment to
// keep the connection alive through proxies and to surface a dead client on the
// next write. A non-positive d disables heartbeats. Defaults to defaultKeepalive.
func WithKeepalive(d time.Duration) HandlerOpt {
	return func(h *handler) { h.keepalive = d }
}

func WithAddress(addr string) HandlerOpt {
	return func(h *handler) { h.addr = addr }
}

// WithListener supplies a pre-bound listener for Serve to use instead of
// binding the configured address itself. Useful for socket activation, binding
// a privileged port before dropping privileges, or tests that need to control
// the listener's lifecycle.
func WithListener(ln net.Listener) HandlerOpt {
	return func(h *handler) { h.ln = ln }
}
