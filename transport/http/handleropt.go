package http

import (
	"net"
	"net/http"
)

type HandlerOpt func(h *handler)

func WithErrorHandler(f func(r *http.Request, err error)) HandlerOpt {
	return func(h *handler) { h.onError = f }
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
