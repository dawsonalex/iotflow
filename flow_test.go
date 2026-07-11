package iotflow_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/dawsonalex/iotflow"
	"github.com/dawsonalex/iotflow/internal/iotflowtest"
	"github.com/stretchr/testify/assert"
)

const (
	testNetSSID = "home-wifi"
	testNetPSK  = "homepassword"
)

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

// newTestFlow creates a Flow backed by p.
func newTestFlow(t *testing.T, p iotflow.Provisioner) *iotflow.Flow {
	t.Helper()
	f, err := iotflow.NewFlow("test", "password", p)
	assert.NoError(t, err)
	return f
}

// --- Begin: terminal-success paths ---

func TestBegin_AlreadyConnected(t *testing.T) {
	f := newTestFlow(t, &iotflowtest.MockProvisioner{
		IsConnectedFn: func(_ context.Context) (bool, error) { return true, nil },
	})

	sub, unsub := f.Subscribe()
	defer unsub()
	results, _ := watchUpdates(t, sub, iotflow.StateConnected)

	assert.NoError(t, f.Begin(t.Context()))
	assert.Equal(t, []iotflow.FlowState{
		iotflow.StateIdle,
		iotflow.StateCheckingConnection,
		iotflow.StateConnected,
	}, stateSeq(<-results))
}

func TestBegin_FullProvisioning(t *testing.T) {
	f := newTestFlow(t, &iotflowtest.MockProvisioner{
		IsConnectedFn: func(_ context.Context) (bool, error) { return false, nil },
		EnableAPModeFn: func(_ context.Context, _, _ string) (<-chan iotflow.ProvisionUpdate, error) {
			return iotflowtest.ConnectedCh(), nil
		},
		ConnectToNetworkFn: func(_ context.Context, _, _ string) (<-chan iotflow.ProvisionUpdate, error) {
			return iotflowtest.ConnectedCh(), nil
		},
	})

	sub, unsub := f.Subscribe()
	defer unsub()
	results, waiting := watchUpdates(t, sub, iotflow.StateWaitingForCredentials)

	beginErr := make(chan error, 1)
	go func() { beginErr <- f.Begin(t.Context()) }()

	<-waiting
	assert.NoError(t, f.Submit(testNetSSID, testNetPSK))

	assert.NoError(t, <-beginErr)
	assert.Equal(t, []iotflow.FlowState{
		iotflow.StateIdle,
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
		IsConnectedFn: func(_ context.Context) (bool, error) { return false, nil },
		EnableAPModeFn: func(_ context.Context, _, _ string) (<-chan iotflow.ProvisionUpdate, error) {
			return iotflowtest.ConnectedCh(), nil
		},
		ConnectToNetworkFn: func(_ context.Context, _, _ string) (<-chan iotflow.ProvisionUpdate, error) {
			if callCount.Add(1) == 1 {
				return iotflowtest.FailedCh(connErr), nil // first attempt fails
			}
			return iotflowtest.ConnectedCh(), nil
		},
	})

	ch, unsub := f.Subscribe()
	defer unsub()

	results, waiting := watchUpdates(t, ch, iotflow.StateWaitingForCredentials)

	beginErr := make(chan error, 1)
	go func() { beginErr <- f.Begin(t.Context()) }()

	// First wait — submit credentials that will fail at the network level.
	<-waiting
	assert.NoError(t, f.Submit(testNetSSID, "wrongpass1"))

	// Flow loops back through StateEnablingAP to StateWaitingForCredentials.
	<-waiting
	assert.NoError(t, f.Submit(testNetSSID, testNetPSK))

	assert.NoError(t, <-beginErr)
	assert.Equal(t, []iotflow.FlowState{
		iotflow.StateIdle,
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
	f := newTestFlow(t, &iotflowtest.MockProvisioner{
		IsConnectedFn: func(_ context.Context) (bool, error) { return false, backendErr },
	})

	ch, unsub := f.Subscribe()
	defer unsub()

	results, _ := watchUpdates(t, ch, iotflow.StateFailed)

	err := f.Begin(t.Context())
	assert.ErrorIs(t, err, backendErr)

	updates := <-results
	assert.Equal(t, []iotflow.FlowState{iotflow.StateIdle, iotflow.StateCheckingConnection, iotflow.StateFailed}, stateSeq(updates))
	assert.ErrorIs(t, updates[len(updates)-1].Err, backendErr)
}

func TestBegin_EnableAPModeError(t *testing.T) {
	backendErr := errors.New("radio blocked")
	f := newTestFlow(t, &iotflowtest.MockProvisioner{
		IsConnectedFn:  func(_ context.Context) (bool, error) { return false, nil },
		EnableAPModeFn: func(_ context.Context, _, _ string) (<-chan iotflow.ProvisionUpdate, error) { return nil, backendErr },
	})

	ch, unsub := f.Subscribe()
	defer unsub()

	results, _ := watchUpdates(t, ch, iotflow.StateFailed)

	err := f.Begin(t.Context())
	assert.ErrorIs(t, err, backendErr)

	updates := <-results
	assert.Equal(t, []iotflow.FlowState{iotflow.StateIdle, iotflow.StateCheckingConnection, iotflow.StateEnablingAP, iotflow.StateFailed}, stateSeq(updates))
}

func TestBegin_DisableAPModeError(t *testing.T) {
	backendErr := errors.New("cannot deactivate")
	f := newTestFlow(t, &iotflowtest.MockProvisioner{
		IsConnectedFn: func(_ context.Context) (bool, error) { return false, nil },
		EnableAPModeFn: func(_ context.Context, _, _ string) (<-chan iotflow.ProvisionUpdate, error) {
			return iotflowtest.ConnectedCh(), nil
		},
		DisableAPModeFn: func() error { return backendErr },
	})

	ch, unsub := f.Subscribe()
	defer unsub()

	results, waiting := watchUpdates(t, ch, iotflow.StateWaitingForCredentials)

	beginErr := make(chan error, 1)
	go func() { beginErr <- f.Begin(t.Context()) }()

	<-waiting
	assert.NoError(t, f.Submit(testNetSSID, testNetPSK))

	err := <-beginErr
	assert.ErrorIs(t, err, backendErr)

	updates := <-results
	assert.Equal(t, []iotflow.FlowState{
		iotflow.StateIdle,
		iotflow.StateCheckingConnection,
		iotflow.StateEnablingAP,
		iotflow.StateWaitingForCredentials,
		iotflow.StateDisablingAP,
		iotflow.StateFailed,
	}, stateSeq(updates))
	assert.ErrorIs(t, updates[len(updates)-1].Err, backendErr)
}

func TestBegin_ContextCancelledWhileWaiting(t *testing.T) {
	f := newTestFlow(t, &iotflowtest.MockProvisioner{
		IsConnectedFn: func(_ context.Context) (bool, error) { return false, nil },
		EnableAPModeFn: func(_ context.Context, _, _ string) (<-chan iotflow.ProvisionUpdate, error) {
			return iotflowtest.ConnectedCh(), nil
		},
	})

	ch, unsub := f.Subscribe()
	defer unsub()

	results, waiting := watchUpdates(t, ch, iotflow.StateWaitingForCredentials)

	ctx, cancel := context.WithCancel(t.Context())
	beginErr := make(chan error, 1)
	go func() { beginErr <- f.Begin(ctx) }()

	<-waiting
	cancel()

	err := <-beginErr
	assert.ErrorIs(t, err, context.Canceled)

	updates := <-results
	last := updates[len(updates)-1]
	assert.Equal(t, iotflow.StateFailed, last.State)
	assert.ErrorIs(t, last.Err, context.Canceled)
}

// --- Submit ---

func TestSubmit_Success(t *testing.T) {
	f := newTestFlow(t, &iotflowtest.MockProvisioner{})
	assert.NoError(t, f.Submit(testNetSSID, testNetPSK))
}

func TestSubmit_InvalidCredentials(t *testing.T) {
	f := newTestFlow(t, &iotflowtest.MockProvisioner{})
	assert.ErrorIs(t, f.Submit(testNetSSID, "short"), iotflow.ErrPSKInvalid)
}

func TestSubmit_AlreadyPending(t *testing.T) {
	f := newTestFlow(t, &iotflowtest.MockProvisioner{})

	assert.NoError(t, f.Submit(testNetSSID, testNetPSK))

	// credsCh buffer (capacity 1) is now full — second submission must be rejected.
	assert.ErrorIs(t, f.Submit(testNetSSID, testNetPSK), iotflow.ErrSubmissionPending)
}

// --- Subscribe lifecycle ---

func TestSubscribe_AfterCompletionIsClosed(t *testing.T) {
	f := newTestFlow(t, &iotflowtest.MockProvisioner{
		IsConnectedFn: func(_ context.Context) (bool, error) { return true, nil },
	})

	assert.NoError(t, f.Begin(t.Context()))

	// Begin has returned; a late subscriber (e.g. an SSE client that connects
	// after provisioning finished) receives the terminal state once and then a
	// close, so it learns the outcome without blocking forever.
	ch, _ := f.Subscribe()

	upd, ok := <-ch
	assert.True(t, ok)
	assert.Equal(t, iotflow.StateConnected, upd.State)

	_, ok = <-ch
	assert.False(t, ok)
}

func TestUnsubscribe_ReleasesChannel(t *testing.T) {
	f := newTestFlow(t, &iotflowtest.MockProvisioner{})

	ch, unsub := f.Subscribe()
	unsub()

	// An unsubscribed channel is closed, so a receive returns the zero value
	// with ok == false rather than blocking. The first value from the channel is the flow state, pop that.
	<-ch
	_, ok := <-ch
	assert.False(t, ok)
}

func TestUnsubscribe_UnknownChannelIsNoop(t *testing.T) {
	f := newTestFlow(t, &iotflowtest.MockProvisioner{})

	_, unsub := f.Subscribe()
	unsub()
	assert.NotPanics(t, func() { unsub() })
}

// --- Provisioner ownership (Finish) ---

func TestFinish_BorrowedProvisionerNotClosed(t *testing.T) {
	var closed atomic.Bool
	// newTestFlow builds the Flow via NewFlow, so the provisioner is borrowed:
	// Finish must leave it for the caller to close.
	f := newTestFlow(t, &iotflowtest.MockProvisioner{
		CloseFn: func() error { closed.Store(true); return nil },
	})

	assert.NoError(t, f.Finish())
	assert.False(t, closed.Load(), "Finish must not close a Provisioner it does not own")
}

// --- NewFlow credential validation ---

func TestNewFlow_InvalidAPCredentials(t *testing.T) {
	_, err := iotflow.NewFlow("", testNetPSK, &iotflowtest.MockProvisioner{})
	assert.ErrorIs(t, err, iotflow.ErrSSIDInvalid)

	_, err = iotflow.NewFlow(testNetSSID, "short", &iotflowtest.MockProvisioner{})
	assert.ErrorIs(t, err, iotflow.ErrPSKInvalid)
}

// --- Begin: ConnectToNetwork synchronous-error paths ---

// TestBegin_RetryOnConnectError covers the branch where ConnectToNetwork returns
// an error directly (flow.go's StateConnecting error return), as opposed to a
// Failed update arriving on the channel — the latter is covered by
// TestBegin_RetryOnConnectionFailure.
func TestBegin_RetryOnConnectError(t *testing.T) {
	connErr := errors.New("activation failed")
	var callCount atomic.Int32

	f := newTestFlow(t, &iotflowtest.MockProvisioner{
		IsConnectedFn: func(_ context.Context) (bool, error) { return false, nil },
		EnableAPModeFn: func(_ context.Context, _, _ string) (<-chan iotflow.ProvisionUpdate, error) {
			return iotflowtest.ConnectedCh(), nil
		},
		ConnectToNetworkFn: func(_ context.Context, _, _ string) (<-chan iotflow.ProvisionUpdate, error) {
			if callCount.Add(1) == 1 {
				return nil, connErr // first attempt fails synchronously
			}
			return iotflowtest.ConnectedCh(), nil
		},
	})

	ch, unsub := f.Subscribe()
	defer unsub()

	results, waiting := watchUpdates(t, ch, iotflow.StateWaitingForCredentials)

	beginErr := make(chan error, 1)
	go func() { beginErr <- f.Begin(t.Context()) }()

	<-waiting
	assert.NoError(t, f.Submit(testNetSSID, "wrongpass1"))

	<-waiting
	assert.NoError(t, f.Submit(testNetSSID, testNetPSK))

	assert.NoError(t, <-beginErr)
	assert.Equal(t, []iotflow.FlowState{
		iotflow.StateIdle,
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

// TestBegin_ContextCancelledDuringConnect covers cancellation on the synchronous
// ConnectToNetwork error path: when ctx is already cancelled, the Flow must fail
// with ctx.Err() rather than looping back into AP mode.
func TestBegin_ContextCancelledDuringConnect(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())

	f := newTestFlow(t, &iotflowtest.MockProvisioner{
		IsConnectedFn: func(_ context.Context) (bool, error) { return false, nil },
		EnableAPModeFn: func(_ context.Context, _, _ string) (<-chan iotflow.ProvisionUpdate, error) {
			return iotflowtest.ConnectedCh(), nil
		},
		ConnectToNetworkFn: func(_ context.Context, _, _ string) (<-chan iotflow.ProvisionUpdate, error) {
			cancel()
			return nil, errors.New("activation failed")
		},
	})

	ch, unsub := f.Subscribe()
	defer unsub()

	results, waiting := watchUpdates(t, ch, iotflow.StateWaitingForCredentials)

	beginErr := make(chan error, 1)
	go func() { beginErr <- f.Begin(ctx) }()

	<-waiting
	assert.NoError(t, f.Submit(testNetSSID, testNetPSK))

	err := <-beginErr
	assert.ErrorIs(t, err, context.Canceled)

	updates := <-results
	last := updates[len(updates)-1]
	assert.Equal(t, iotflow.StateFailed, last.State)
	assert.ErrorIs(t, last.Err, context.Canceled)
}

// TestBegin_ContextCancelledDuringConnectChannel covers the same cancellation
// guarantee on the channel path: ConnectToNetwork succeeds but its update channel
// never reaches a terminal state, and ctx is cancelled while draining it.
func TestBegin_ContextCancelledDuringConnectChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	blocked := make(chan iotflow.ProvisionUpdate) // never sends, never closes

	f := newTestFlow(t, &iotflowtest.MockProvisioner{
		IsConnectedFn: func(_ context.Context) (bool, error) { return false, nil },
		EnableAPModeFn: func(_ context.Context, _, _ string) (<-chan iotflow.ProvisionUpdate, error) {
			return iotflowtest.ConnectedCh(), nil
		},
		ConnectToNetworkFn: func(_ context.Context, _, _ string) (<-chan iotflow.ProvisionUpdate, error) {
			return blocked, nil
		},
	})

	ch, unsub := f.Subscribe()
	defer unsub()

	results, waiting := watchUpdates(t, ch, iotflow.StateConnecting)

	beginErr := make(chan error, 1)
	go func() { beginErr <- f.Begin(ctx) }()

	assert.NoError(t, f.Submit(testNetSSID, testNetPSK))

	<-waiting // Flow is now blocked draining the connect channel
	cancel()

	err := <-beginErr
	assert.ErrorIs(t, err, context.Canceled)

	updates := <-results
	last := updates[len(updates)-1]
	assert.Equal(t, iotflow.StateFailed, last.State)
	assert.ErrorIs(t, last.Err, context.Canceled)
}

// TestBegin_DropsStaleCredentialsOnRetry verifies the drainCreds step: a
// credential that arrives mid-connect must be discarded on retry rather than
// silently reused, so the next connection uses fresh credentials.
func TestBegin_DropsStaleCredentialsOnRetry(t *testing.T) {
	connErr := errors.New("authentication failed")
	var callCount atomic.Int32
	var secondConnectSSID string

	var f *iotflow.Flow
	f = newTestFlow(t, &iotflowtest.MockProvisioner{
		IsConnectedFn: func(_ context.Context) (bool, error) { return false, nil },
		EnableAPModeFn: func(_ context.Context, _, _ string) (<-chan iotflow.ProvisionUpdate, error) {
			return iotflowtest.ConnectedCh(), nil
		},
		ConnectToNetworkFn: func(_ context.Context, ssid, _ string) (<-chan iotflow.ProvisionUpdate, error) {
			if callCount.Add(1) == 1 {
				// A stale credential arrives while the first connect is in
				// flight; the retry must drop it instead of reusing it.
				_ = f.Submit("stale-net", "stalepassword")
				return iotflowtest.FailedCh(connErr), nil
			}
			secondConnectSSID = ssid
			return iotflowtest.ConnectedCh(), nil
		},
	})

	ch, unsub := f.Subscribe()
	defer unsub()

	_, waiting := watchUpdates(t, ch, iotflow.StateWaitingForCredentials)

	beginErr := make(chan error, 1)
	go func() { beginErr <- f.Begin(t.Context()) }()

	<-waiting
	assert.NoError(t, f.Submit(testNetSSID, testNetPSK))

	// Second wait: by now the retry has run drainCreds, so the stale submission
	// is gone and the Flow is blocked waiting for a fresh credential.
	<-waiting
	assert.NoError(t, f.Submit("fresh-net", "freshpassword"))

	assert.NoError(t, <-beginErr)
	assert.Equal(t, "fresh-net", secondConnectSSID,
		"retry must use the freshly submitted credential, not the stale mid-connect one")
}
