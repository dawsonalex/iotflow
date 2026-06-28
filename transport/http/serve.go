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

func Serve(ctx context.Context, f *iotflow.Flow, opts ...HandlerOpt) error {
	h := &handler{flow: f, addr: ":80"}
	for _, o := range opts {
		o(h)
	}

	ln, err := net.Listen("tcp", h.addr)
	if err != nil {
		return fmt.Errorf("binding %s: %w", h.addr, err)
	}

	srv := &http.Server{Addr: h.addr, Handler: h.mux()}
	serveErrChan := make(chan error, 1)
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) && h.onError != nil {
			//h.onError(nil, err)
			serveErrChan <- err
		}
	}()

	// The Flow drives the lifecycle; when Begin returns the AP is gone, so
	// the server has nothing left to serve — shut it down.
	flowErrChan := make(chan error, 1)
	go func() {
		err := f.Begin(ctx)
		flowErrChan <- err
	}()

	var doneErr error
	for {
		select {
		case err := <-serveErrChan:
			doneErr = err
			break
		case err := <-flowErrChan:
			doneErr = err
			break
		case <-ctx.Done():
			doneErr = ctx.Err()
			break
		}
	}

	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutCtx)

	return beginErr
}
