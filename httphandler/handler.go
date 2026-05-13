package httphandler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/dawsonalex/iotflow"
)

var ValidationError = errors.New("validation error")

// HandlerProvisioner is an interface that encapsulates the provisioner methods required by the handler.
type HandlerProvisioner interface {
	IsConnected(ctx context.Context) (bool, error)
	EnableAPMode(ctx context.Context, ssid, psk string) (<-chan iotflow.ProvisionUpdate, error)
	DisableAPMode() error
	ConnectToNetwork(ctx context.Context, ssid, psk string) (<-chan iotflow.ProvisionUpdate, error)
}

type errHandler func(r *http.Request, err error)

type handler struct {
	mu          sync.RWMutex
	provisioner HandlerProvisioner
	onError     errHandler
	// TODO: probably add a logger here for enriching errors like event encoding.
}

type statusResponse struct {
	Connected bool `json:"connected"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func (h *handler) handleStatus() http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		h.mu.RLock()
		defer h.mu.RUnlock()

		connected, err := h.provisioner.IsConnected(r.Context())
		if err != nil {
			rw.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(rw).Encode(errorResponse{err.Error()})

			if h.onError != nil {
				h.onError(r, err)
			}
			return
		}

		_ = json.NewEncoder(rw).Encode(statusResponse{connected})
	}
}

type apModeRequest struct {
	SSID string `json:"ssid"`
	PSK  string `json:"psk"`
}

func (h *handler) handleEnableApMode() http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		h.mu.Lock()
		defer h.mu.Unlock()

		reqBody := apModeRequest{}
		if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
			http.Error(rw, err.Error(), http.StatusBadRequest)

			if h.onError != nil {
				h.onError(r, fmt.Errorf("%w: decoding request body: %w", ValidationError, err))
			}
			return
		}

		updChan, err := h.provisioner.EnableAPMode(r.Context(), reqBody.SSID, reqBody.PSK)
		if err != nil {
			if errors.Is(err, iotflow.ErrSSIDInvalid) || errors.Is(err, iotflow.ErrPSKInvalid) {
				http.Error(rw, err.Error(), http.StatusBadRequest)

				// Re-assign the error here so that the wrapped error is passed to onError
				err = fmt.Errorf("%w: %w", ValidationError, err)
			} else {
				http.Error(rw, err.Error(), http.StatusInternalServerError)
			}

			if h.onError != nil {
				h.onError(r, err)
			}
			return
		}

		sseResponse(rw, r, updChan, h.onError)
	}
}

func (h *handler) handleDisableApMode() http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		h.mu.Lock()
		defer h.mu.Unlock()

		if err := h.provisioner.DisableAPMode(); err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)

			if h.onError != nil {
				h.onError(r, err)
			}
			return
		}

		rw.WriteHeader(http.StatusNoContent)
	}
}

func (h *handler) handleConnectToNetwork() http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		h.mu.Lock()
		defer h.mu.Unlock()

		reqBody := apModeRequest{}
		if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
			http.Error(rw, err.Error(), http.StatusBadRequest)

			if h.onError != nil {
				h.onError(r, fmt.Errorf("%w: decoding request body: %w", ValidationError, err))
			}
			return
		}

		updChan, err := h.provisioner.ConnectToNetwork(r.Context(), reqBody.SSID, reqBody.PSK)
		if err != nil {
			if errors.Is(err, iotflow.ErrSSIDInvalid) || errors.Is(err, iotflow.ErrPSKInvalid) {
				http.Error(rw, err.Error(), http.StatusBadRequest)

				// Re-assign the error here so that the wrapped error is passed to onError
				err = fmt.Errorf("%w: %w", ValidationError, err)
			} else {
				http.Error(rw, err.Error(), http.StatusInternalServerError)
			}

			if h.onError != nil {
				h.onError(r, err)
			}
			return
		}

		sseResponse(rw, r, updChan, h.onError)
	}
}

// sseResponse listens to tChan and writes JSON marshaled values to rw in SSE format, flushing rw
// after each event. If an error occurs, it is passed to errHandler (errHandler may be nil).
// r.Context() is used to cancel the channel loop.
func sseResponse[T any](rw http.ResponseWriter, r *http.Request, tChan <-chan T, errHandler errHandler) {
	rw.Header().Set("Content-Type", "text/event-stream")
	rw.Header().Set("Cache-Control", "no-cache")
	var tVal T
	for {
		select {
		case <-r.Context().Done():
			return
		case t, ok := <-tChan:
			if !ok {
				return
			}
			tVal = t
		}

		payload, err := json.Marshal(tVal)
		if err != nil && errHandler != nil {
			errHandler(r, err)
			return
		}
		_, _ = rw.Write([]byte("data: " + string(payload) + "\n\n"))
		rw.(http.Flusher).Flush()
	}
}

func New(p HandlerProvisioner, opts ...HandlerOpt) http.Handler {
	h := &handler{provisioner: p}

	for _, o := range opts {
		o(h)
	}

	mux := http.NewServeMux()
	mux.Handle("GET /status", h.handleStatus())
	mux.Handle("POST /ap", h.handleEnableApMode())
	mux.Handle("DELETE /ap", h.handleDisableApMode())
	mux.Handle("POST /connect", h.handleConnectToNetwork())

	return mux
}
