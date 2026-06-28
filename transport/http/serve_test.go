package http

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/dawsonalex/iotflow"
	"github.com/dawsonalex/iotflow/internal/iotflowtest"
	"github.com/stretchr/testify/assert"
)

// newWaitingFlow returns a Flow whose provisioner drives it as far as
// StateWaitingForCredentials and then parks there: IsConnected reports false and
// EnableAPMode succeeds immediately, so Begin blocks waiting for a Submit that
// the test never makes. That leaves cancellation of the context as the only way
// Begin (and therefore Serve) can return — exactly what the shutdown path tests.
func newWaitingFlow(t *testing.T) *iotflow.Flow {
	t.Helper()
	f, err := iotflow.NewFlow("test", "password", &iotflowtest.MockProvisioner{
		IsConnectedFn: func(_ context.Context) (bool, error) { return false, nil },
		EnableAPModeFn: func(_ context.Context, _, _ string) (<-chan iotflow.ProvisionUpdate, error) {
			return iotflowtest.ConnectedCh(), nil
		},
	})
	assert.NoError(t, err)
	return f
}

func TestServeCtxShutdown(t *testing.T) {
	f := newWaitingFlow(t)

	ctx, cancel := context.WithCancel(t.Context())

	serveErr := make(chan error, 1)
	go func() {
		// :0 lets the OS pick a free port so the test never contends for :80.
		serveErr <- Serve(ctx, f, WithAddress("127.0.0.1:0"))
	}()

	cancel()

	select {
	case err := <-serveErr:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after context cancellation")
	}
}

// TestServeServerError exercises the server-error branch of the select: the
// server dies while the Flow is still parked waiting for credentials. Serve
// must surface the server's error — not the Flow's self-inflicted cancellation,
// which joinNonCancel drops.
func TestServeServerError(t *testing.T) {
	f := newWaitingFlow(t)

	// A listener the test controls, handed to Serve via WithListener.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- Serve(t.Context(), f, WithListener(ln))
	}()

	// Close the listener out from under the running server. srv.Serve's Accept
	// then fails with net.ErrClosed, which drives the server-error branch
	// regardless of whether Serve has reached srv.Serve(ln) yet.
	assert.NoError(t, ln.Close())

	select {
	case err := <-serveErr:
		assert.Error(t, err)
		assert.ErrorIs(t, err, net.ErrClosed)       // the server's real failure
		assert.NotErrorIs(t, err, context.Canceled) // not the flow teardown we triggered
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after the server stopped")
	}
}
