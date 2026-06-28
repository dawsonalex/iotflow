package iotflow_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/dawsonalex/iotflow"
	"github.com/stretchr/testify/assert"
)

const (
	testAPSSID  = "test-ap"
	testAPPSK   = "testpass" // exactly 8 chars — minimum valid PSK
	testNetSSID = "home-wifi"
	testNetPSK  = "homepassword"
)

// connectedCh returns a closed channel carrying a single Connected update.
func connectedCh() <-chan iotflow.ProvisionUpdate {
	ch := make(chan iotflow.ProvisionUpdate, 1)
	ch <- iotflow.ProvisionUpdate{State: iotflow.ProvisionStateConnected}
	close(ch)
	return ch
}

// failedCh returns a closed channel carrying a single Failed update.
func failedCh(err error) <-chan iotflow.ProvisionUpdate {
	ch := make(chan iotflow.ProvisionUpdate, 1)
	ch <- iotflow.ProvisionUpdate{State: iotflow.ProvisionStateFailed, Err: err}
	close(ch)
	return ch
}

// watchUpdates starts a goroutine that collects all FlowUpdates from ch. Each
// time the target state is observed, a struct is sent on the returned signals
// channel. When ch closes, the full update slice is sent on results.
func watchUpdates(t *testing.T, ch <-chan iotflow.FlowUpdate, target iotflow.FlowState) (results <-chan []iotflow.FlowUpdate, signals <-chan struct{}) {
	t.Helper()
	resultCh := make(chan []iotflow.FlowUpdate, 1)
	signalCh := make(chan struct{}, 8)
	go func() {
		var all []iotflow.FlowUpdate
		for u := range ch {
			all = append(all, u)
			if u.State == target {
				signalCh <- struct{}{}
			}
		}
		resultCh <- all
	}()
	return resultCh, signalCh
}

// stateSeq extracts just the State field from each FlowUpdate.
func stateSeq(updates []iotflow.FlowUpdate) []iotflow.FlowState {
	states := make([]iotflow.FlowState, len(updates))
	for i, u := range updates {
		states[i] = u.State
	}
	return states
}

// newTestFlow creates a Flow backed by b using the shared test AP credentials.
func newTestFlow(t *testing.T, p Provisioner) *Flow {
	t.Helper()
	f, err := NewFlow(p)
	assert.NoError(t, err)
	return f
}

// --- Begin: terminal-success paths ---

func TestBegin_AlreadyConnected(t *testing.T) {
	f := newTestFlow(t, &mockBackend{
		isConnectedFn: func(_ context.Context) (bool, error) { return true, nil },
	})

	results, _ := watchUpdates(t, f.Subscribe(), StateConnected)

	assert.NoError(t, f.Begin(t.Context()))
	assert.Equal(t, []iotflow.FlowState{
		iotflow.StateCheckingConnection,
		StateConnected,
	}, stateSeq(<-results))
}

func TestBegin_FullProvisioning(t *testing.T) {
	f := newTestFlow(t, &mockBackend{
		isConnectedFn: func(_ context.Context) (bool, error) { return false, nil },
		enableAPModeFn: func(_ context.Context, _, _ string) (<-chan iotflow.ProvisionUpdate, error) {
			return connectedCh(), nil
		},
		connectToNetworkFn: func(_ context.Context, _, _ string) (<-chan iotflow.ProvisionUpdate, error) {
			return connectedCh(), nil
		},
	})

	results, waiting := watchUpdates(t, f.Subscribe(), iotflow.StateWaitingForCredentials)

	beginErr := make(chan error, 1)
	go func() { beginErr <- f.Begin(t.Context()) }()

	<-waiting
	assert.NoError(t, f.Submit(testNetSSID, testNetPSK))

	assert.NoError(t, <-beginErr)
	assert.Equal(t, []iotflow.FlowState{
		iotflow.StateCheckingConnection,
		iotflow.StateEnablingAP,
		iotflow.StateWaitingForCredentials,
		iotflow.StateDisablingAP,
		iotflow.StateConnecting,
		iotflow.StateProvisioned,
	}, stateSeq(<-results))
}

// --- Begin: retry path ---

func TestBegin_RetryOnConnectionFailure(t *testing.T) {
	connErr := errors.New("authentication failed")
	var callCount atomic.Int32

	f := newTestFlow(t, &iotflowtest.MockProvisioner{
		isConnectedFn: func(_ context.Context) (bool, error) { return false, nil },
		enableAPModeFn: func(_ context.Context, _, _ string) (<-chan iotflow.ProvisionUpdate, error) {
			return connectedCh(), nil
		},
		connectToNetworkFn: func(_ context.Context, _, _ string) (<-chan iotflow.ProvisionUpdate, error) {
			if callCount.Add(1) == 1 {
				return failedCh(connErr), nil // first attempt fails
			}
			return connectedCh(), nil
		},
	})

	results, waiting := watchUpdates(t, f.Subscribe(), iotflow.StateWaitingForCredentials)

	beginErr := make(chan error, 1)
	go func() { beginErr <- f.Begin(t.Context()) }()

	// First wait — submit credentials that will fail at the network level.
	<-waiting
	assert.NoError(t, f.Submit(testNetSSID, "wrongpass1"))

	// Flow loops back through iotflow.StateEnablingAP to StateWaitingForCredentials.
	<-waiting
	assert.NoError(t, f.Submit(testNetSSID, testNetPSK))

	assert.NoError(t, <-beginErr)
	assert.Equal(t, []iotflow.FlowState{
		iotflow.StateCheckingConnection,
		iotflow.StateEnablingAP,
		iotflow.StateWaitingForCredentials,
		iotflow.StateDisablingAP,
		iotflow.StateConnecting,
		iotflow.StateEnablingAP, // retry loop
		iotflow.StateWaitingForCredentials,
		iotflow.StateDisablingAP,
		iotflow.StateConnecting,
		iotflow.StateProvisioned,
	}, stateSeq(<-results))
}

// --- Begin: error paths ---

func TestBegin_IsConnectedError(t *testing.T) {
	backendErr := errors.New("dbus error")
	f := newTestFlow(t, &mockBackend{
		isConnectedFn: func(_ context.Context) (bool, error) { return false, backendErr },
	})

	results, _ := watchUpdates(t, f.Subscribe(), StateFailed)

	err := f.Begin(t.Context())
	assert.ErrorIs(t, err, backendErr)

	updates := <-results
	assert.Equal(t, []iotflow.FlowState{iotflow.StateCheckingConnection, StateFailed}, stateSeq(updates))
	assert.ErrorIs(t, updates[len(updates)-1].Err, backendErr)
}

func TestBegin_EnableAPModeError(t *testing.T) {
	backendErr := errors.New("radio blocked")
	f := newTestFlow(t, &mockBackend{
		isConnectedFn:  func(_ context.Context) (bool, error) { return false, nil },
		enableAPModeFn: func(_ context.Context, _, _ string) (<-chan iotflow.ProvisionUpdate, error) { return nil, backendErr },
	})

	results, _ := watchUpdates(t, f.Subscribe(), StateFailed)

	err := f.Begin(t.Context())
	assert.ErrorIs(t, err, backendErr)

	updates := <-results
	assert.Equal(t, []iotflow.FlowState{iotflow.StateCheckingConnection, iotflow.StateEnablingAP, StateFailed}, stateSeq(updates))
}

func TestBegin_DisableAPModeError(t *testing.T) {
	backendErr := errors.New("cannot deactivate")
	f := newTestFlow(t, &mockBackend{
		isConnectedFn: func(_ context.Context) (bool, error) { return false, nil },
		enableAPModeFn: func(_ context.Context, _, _ string) (<-chan iotflow.ProvisionUpdate, error) {
			return connectedCh(), nil
		},
		disableAPModeFn: func() error { return backendErr },
	})

	results, waiting := watchUpdates(t, f.Subscribe(), iotflow.StateWaitingForCredentials)

	beginErr := make(chan error, 1)
	go func() { beginErr <- f.Begin(t.Context()) }()

	<-waiting
	assert.NoError(t, f.Submit(testNetSSID, testNetPSK))

	err := <-beginErr
	assert.ErrorIs(t, err, backendErr)

	updates := <-results
	assert.Equal(t, []iotflow.FlowState{
		iotflow.StateCheckingConnection,
		iotflow.StateEnablingAP,
		iotflow.StateWaitingForCredentials,
		iotflow.StateDisablingAP,
		StateFailed,
	}, stateSeq(updates))
	assert.ErrorIs(t, updates[len(updates)-1].Err, backendErr)
}

func TestBegin_ContextCancelledWhileWaiting(t *testing.T) {
	f := newTestFlow(t, &mockBackend{
		isConnectedFn: func(_ context.Context) (bool, error) { return false, nil },
		enableAPModeFn: func(_ context.Context, _, _ string) (<-chan iotflow.ProvisionUpdate, error) {
			return connectedCh(), nil
		},
	})

	results, waiting := watchUpdates(t, f.Subscribe(), iotflow.StateWaitingForCredentials)

	ctx, cancel := context.WithCancel(t.Context())
	beginErr := make(chan error, 1)
	go func() { beginErr <- f.Begin(ctx) }()

	<-waiting
	cancel()

	err := <-beginErr
	assert.ErrorIs(t, err, context.Canceled)

	updates := <-results
	last := updates[len(updates)-1]
	assert.Equal(t, StateFailed, last.State)
	assert.ErrorIs(t, last.Err, context.Canceled)
}

// --- Submit ---

func TestSubmit_Success(t *testing.T) {
	f := newTestFlow(t, &mockBackend{})
	assert.NoError(t, f.Submit(testNetSSID, testNetPSK))
}

func TestSubmit_InvalidCredentials(t *testing.T) {
	f := newTestFlow(t, &mockBackend{})
	assert.ErrorIs(t, f.Submit(testNetSSID, "short"), ErrPSKInvalid)
}

func TestSubmit_AlreadyPending(t *testing.T) {
	f := newTestFlow(t, &mockBackend{})

	assert.NoError(t, f.Submit(testNetSSID, testNetPSK))

	// credsCh buffer (capacity 1) is now full — second submission must be rejected.
	assert.ErrorIs(t, f.Submit(testNetSSID, testNetPSK), ErrSubmissionPending)
}

// --- Subscribe lifecycle ---

func TestSubscribe_AfterCompletionIsClosed(t *testing.T) {
	f := newTestFlow(t, &mockBackend{
		isConnectedFn: func(_ context.Context) (bool, error) { return true, nil },
	})

	assert.NoError(t, f.Begin(t.Context()))

	// Begin has returned; new subscriptions must be already-closed so callers
	// (e.g. a late SSE client) don't block forever.
	ch := f.Subscribe()
	_, ok := <-ch
	assert.False(t, ok)
}
