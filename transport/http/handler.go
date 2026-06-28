package http

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"

	"github.com/dawsonalex/iotflow"
)

var ErrValidation = errors.New("validation error")

type errHandler func(r *http.Request, err error)

type Flow interface {
	Submit(ssid, psk string) error
	Subscribe() <-chan iotflow.FlowUpdate
	Unsubscribe(<-chan iotflow.FlowUpdate)
}

type handler struct {
	flow    Flow
	addr    string
	ln      net.Listener // when set, used in place of binding addr
	onError errHandler
}

type credentialsRequest struct {
	SSID string `json:"ssid"`
	PSK  string `json:"psk"`
}

func (h *handler) handlePostCredentials() http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		reqBody := credentialsRequest{}
		if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
			http.Error(rw, err.Error(), http.StatusBadRequest)

			if h.onError != nil {
				h.onError(r, fmt.Errorf("%w: decoding request body: %w", ErrValidation, err))
			}
			return
		}

		err := h.flow.Submit(reqBody.SSID, reqBody.PSK)
		switch {
		case err == nil:
			rw.WriteHeader(http.StatusAccepted) // 202 — acked while AP is still up
		case errors.Is(err, iotflow.ErrSubmissionPending):
			http.Error(rw, err.Error(), http.StatusConflict) // 409
		case errors.Is(err, iotflow.ErrSSIDInvalid), errors.Is(err, iotflow.ErrPSKInvalid):
			http.Error(rw, err.Error(), http.StatusBadRequest) // 400
		default:
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			if h.onError != nil {
				h.onError(r, err)
			}
		}
	}
}

func (h *handler) handleGetEvents() http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		// TODO: Check how this works with context timeouts, and in general how to stop the subscription.
		ch := h.flow.Subscribe()
		defer h.flow.Unsubscribe(ch)
		doSseResponse(rw, r, ch, h.onError)
	}
}

func (h *handler) mux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("POST /credentials", h.handlePostCredentials())
	mux.Handle("GET /events", h.handleGetEvents())
	return mux
}

func NewHandler(f Flow, opts ...HandlerOpt) http.Handler {
	h := &handler{flow: f}

	for _, o := range opts {
		o(h)
	}

	return h.mux()
}
