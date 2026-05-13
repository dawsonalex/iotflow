package httphandler

import "net/http"

type HandlerOpt func(*handler)

func WithErrorHandler(f func(r *http.Request, err error)) HandlerOpt {
	return func(h *handler) { h.onError = f }
}
