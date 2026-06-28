package http

import (
	"net/http"
)

type HandlerOpt func(h *handler)

func WithErrorHandler(f func(r *http.Request, err error)) HandlerOpt {
	return func(h *handler) { h.onError = f }
}

func WithAddress(addr string) HandlerOpt {
	return func(h *handler) { h.addr = addr }
}
