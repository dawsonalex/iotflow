package http

import (
	"net"
	"net/http"
	"regexp"
	"strings"
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

var multiSlashRegex = regexp.MustCompile("/{2,}")

// WithPrefix sets a path prefix for the handler's routes, so the endpoints can
// be mounted under that prefix in a parent mux (e.g. "/provision" serves
// POST /provision/credentials and GET /provision/events) without wrapping the
// handler in http.StripPrefix. The prefix is normalized: leading and trailing
// slashes don't matter, and the empty string leaves the routes flat (the
// default).
func WithPrefix(prefix string) HandlerOpt {
	return func(h *handler) {
		prefix = strings.Trim(prefix, "/")
		prefix = multiSlashRegex.ReplaceAllString(prefix, "/")
		if prefix == "" {
			return
		}

		h.routePrefix = "/" + prefix
	}
}
