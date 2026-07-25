package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/dawsonalex/iotflow"
	"github.com/dawsonalex/iotflow/provision"
)

var ErrValidation = errors.New("validation error")

// defaultKeepalive is how often an idle SSE stream emits a heartbeat comment.
// A provisioning flow can sit in StateWaitingForCredentials indefinitely with no
// updates, so without this the connection looks dead to proxies and NATs.
const defaultKeepalive = 15 * time.Second

type errHandler func(r *http.Request, err error)

type Flow interface {
	Submit(ssid, psk string) error
	Subscribe() (<-chan iotflow.FlowUpdate, func())
	ListAccessPoints(ctx context.Context) ([]provision.Network, error)
}

type handler struct {
	flow        Flow
	addr        string
	routePrefix string
	ln          net.Listener // when set, used in place of binding addr
	onError     errHandler
	keepalive   time.Duration // SSE heartbeat interval; see defaultKeepalive
}

type errorResponse struct {
	Code  string `json:"code"`
	Error string `json:"error"`
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

			h.handleErr(r, fmt.Errorf("%w: decoding request body: %w", ErrValidation, err))
			return
		}

		err := h.flow.Submit(reqBody.SSID, reqBody.PSK)
		if err != nil {
			errRes := errorResponse{Code: "credentials_error", Error: err.Error()}
			statusCode := http.StatusInternalServerError

			switch {
			case errors.Is(err, iotflow.ErrNotAwaitingCredentials):
				errRes.Code = "ErrNotAwaitingCredentials"
				statusCode = http.StatusConflict
			case errors.Is(err, iotflow.ErrSubmissionPending):
				errRes.Code = "ErrSubmissionPending"
				statusCode = http.StatusConflict
			case errors.Is(err, provision.ErrSSIDInvalid), errors.Is(err, provision.ErrPSKInvalid):
				errRes.Code = "ErrInvalidCredentials"
				statusCode = http.StatusBadRequest
			}

			// Header and status must both be written before the body: the first
			// write to rw implicitly commits 200, which would discard statusCode.
			rw.Header().Set("Content-Type", "application/json")
			rw.WriteHeader(statusCode)
			if encErr := json.NewEncoder(rw).Encode(errRes); encErr != nil {
				// The status line is already on the wire, so this can only be
				// reported, not turned into a 500 response.
				h.handleErr(r, fmt.Errorf("encoding error response: %w", encErr))
			}

			h.handleErr(r, err)
			return
		}

		rw.WriteHeader(http.StatusAccepted)
	}
}

func (h *handler) handleGetEvents() http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		ch, unsubscribe := h.flow.Subscribe()
		defer unsubscribe()
		doSseResponse(rw, r, ch, h.keepalive, h.onError)
	}
}

type networkResponse struct {
	Networks []provision.Network `json:"networks"`
}

func (h *handler) handleGetAps() http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		aps, err := h.flow.ListAccessPoints(r.Context())
		if err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			h.handleErr(r, err)
			return
		}
		resp := networkResponse{Networks: aps}
		if err := json.NewEncoder(rw).Encode(resp); err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			h.handleErr(r, err)
		}
	}
}

func (h *handler) handleErr(r *http.Request, err error) {
	if h.onError != nil {
		h.onError(r, err)
	}
}

func (h *handler) mux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("POST "+h.routePrefix+"/credentials", h.handlePostCredentials())
	mux.Handle("GET "+h.routePrefix+"/events", h.handleGetEvents())
	mux.Handle("GET "+h.routePrefix+"/aps", h.handleGetAps())
	return mux
}

func NewHandler(f Flow, opts ...HandlerOpt) http.Handler {
	h := &handler{flow: f, keepalive: defaultKeepalive}

	for _, o := range opts {
		o(h)
	}

	return h.mux()
}
