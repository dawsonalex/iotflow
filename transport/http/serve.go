package http

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/dawsonalex/iotflow"
)

// Serve runs the provisioning HTTP server and the Flow together, returning when
// the first of them finishes, or ctx is canceled. The server is bound to :80
// by default.
func Serve(ctx context.Context, f *iotflow.Flow, opts ...HandlerOpt) error {
	h := &handler{flow: f, addr: ":80"}
	for _, o := range opts {
		o(h)
	}

	// Default listener if one hasn't been set using an option
	ln := h.ln
	if ln == nil {
		var err error
		ln, err = net.Listen("tcp", h.addr)
		if err != nil {
			return fmt.Errorf("binding %s: %w", h.addr, err)
		}
	}

	// This context manages both the Flow and TCP server.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	srv := &http.Server{Handler: h.mux()}

	serveErrChan := make(chan error, 1)
	go func() {
		err := srv.Serve(ln)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil // expected on Shutdown; not a failure
		}
		serveErrChan <- err
	}()

	flowErrChan := make(chan error, 1)
	go func() { flowErrChan <- f.Begin(ctx) }()

	var retErr error
	select {
	case fErr := <-flowErrChan:
		// Flow reached a terminal state — success, failure, or caller
		// cancellation (Begin surfaces ctx.Err()). The AP is gone either way,
		// so drain the still-running server gracefully.
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = srv.Shutdown(shutCtx)
		shutCancel()

		// Shutdown blocks until Serve returns, so serveErrChan is ready now.
		// It's normally nil; non-nil means the server hit a real error around
		// the same moment the flow finished — surface it alongside fErr.
		retErr = joinNonCancel(fErr, <-serveErrChan)

	case sErr := <-serveErrChan:
		// Server stopped on its own (Serve closes its listener on return), so
		// there's no server left to drain. Stop the flow and wait for it to
		// unwind; its error is usually the cancellation we just triggered, so
		// surface it only if it's an independent failure.
		cancel()
		retErr = joinNonCancel(sErr, <-flowErrChan)
	}

	return retErr
}

// joinNonCancel combines a primary error with a secondary one, ignoring cases
// where secondary is nil or a context.Cancelled.
func joinNonCancel(primary, secondary error) error {
	if secondary == nil || errors.Is(secondary, context.Canceled) {
		return primary
	}
	return errors.Join(primary, secondary)
}
