package iotflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dawsonalex/iotflow"
	"github.com/dawsonalex/iotflow/internal/iotflowtest"
	"github.com/dawsonalex/iotflow/provision"
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

// waitTimeout bounds every blocking receive in this file. The Flow's mocks are
// all in-memory, so any wait longer than this means the state machine is stuck,
// not slow.
const waitTimeout = 5 * time.Second

// waitFor receives one value from ch, failing the test rather than blocking
// forever if it doesn't arrive. Without this a mis-ordered Submit (one issued
// before the Flow reaches StateWaitingForCredentials, which Submit now rejects)
// parks the state machine and stalls the whole package until the go test
// timeout, hiding the actual failure.
func waitFor[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(waitTimeout):
		t.Fatalf("timed out after %s waiting for %s", waitTimeout, what)
		var zero T
		return zero
	}
}

// stateSeq extracts just the State field from each FlowUpdate.
func stateSeq(updates []iotflow.FlowUpdate) []iotflow.FlowState {
	states := make([]iotflow.FlowState, len(updates))
	for i, u := range updates {
		states[i] = u.State
	}
	return states
}

// firstWithState returns the first update carrying state s. It lets a test
// assert on an update's Err or Reason without pinning it to an index in the
// expected sequence, which matters for the retry paths — those emit some states
// more than once.
func firstWithState(updates []iotflow.FlowUpdate, s iotflow.FlowState) (iotflow.FlowUpdate, bool) {
	for _, u := range updates {
		if u.State == s {
			return u, true
		}
	}
	return iotflow.FlowUpdate{}, false
}

// newTestFlow creates a Flow backed by p.
func newTestFlow(t *testing.T, p provision.Provisioner) *iotflow.Flow {
	t.Helper()
	f, err := iotflow.NewFlow("test", "password", p)
	assert.NoError(t, err)
	return f
}

// --- Begin: terminal-success paths ---

// TestBegin_AlreadyConnected covers the short-circuit path. It reaches the same
// terminal StateProvisioned as a full run — a subscriber learns only that the
// device is on a network, not which route it took there — but does so without
// ever bringing the AP up.
func TestBegin_AlreadyConnected(t *testing.T) {
	f := newTestFlow(t, &iotflowtest.MockProvisioner{
		IsConnectedFn: func(_ context.Context) (bool, error) { return true, nil },
	})

	sub, unsub := f.Subscribe()
	defer unsub()
	results, _ := watchUpdates(t, sub, iotflow.StateProvisioned)

	assert.NoError(t, f.Begin(t.Context()))
	assert.Equal(t, []iotflow.FlowState{
		iotflow.StateIdle,
		iotflow.StateCheckingConnection,
		iotflow.StateProvisioned,
	}, stateSeq(waitFor(t, results, "the update stream to close")))
}

func TestBegin_FullProvisioning(t *testing.T) {
	f := newTestFlow(t, &iotflowtest.MockProvisioner{
		IsConnectedFn: func(_ context.Context) (bool, error) { return false, nil },
		EnableAPModeFn: func(_ context.Context, _, _ string) (<-chan provision.Update, error) {
			return iotflowtest.ConnectedCh(), nil
		},
		ConnectToNetworkFn: func(_ context.Context, _, _ string) (<-chan provision.Update, error) {
			return iotflowtest.ConnectedCh(), nil
		},
	})

	sub, unsub := f.Subscribe()
	defer unsub()
	results, waiting := watchUpdates(t, sub, iotflow.StateWaitingForCredentials)

	beginErr := make(chan error, 1)
	go func() { beginErr <- f.Begin(t.Context()) }()

	waitFor(t, waiting, "StateWaitingForCredentials")
	assert.NoError(t, f.Submit(testNetSSID, testNetPSK))

	assert.NoError(t, waitFor(t, beginErr, "Begin to return"))
	assert.Equal(t, []iotflow.FlowState{
		iotflow.StateIdle,
		iotflow.StateCheckingConnection,
		iotflow.StateEnablingAP,
		iotflow.StateWaitingForCredentials,
		iotflow.StateDisablingAP,
		iotflow.StateConnecting,
		iotflow.StateProvisioned,
	}, stateSeq(waitFor(t, results, "the update stream to close")))
}

// --- Begin: retry path ---

func TestBegin_RetryOnConnectionFailure(t *testing.T) {
	connErr := errors.New("authentication failed")
	var callCount atomic.Int32

	f := newTestFlow(t, &iotflowtest.MockProvisioner{
		IsConnectedFn: func(_ context.Context) (bool, error) { return false, nil },
		EnableAPModeFn: func(_ context.Context, _, _ string) (<-chan provision.Update, error) {
			return iotflowtest.ConnectedCh(), nil
		},
		ConnectToNetworkFn: func(_ context.Context, _, _ string) (<-chan provision.Update, error) {
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
	waitFor(t, waiting, "StateWaitingForCredentials")
	assert.NoError(t, f.Submit(testNetSSID, "wrongpass1"))

	// Flow loops back through StateEnablingAP to StateWaitingForCredentials.
	waitFor(t, waiting, "StateWaitingForCredentials")
	assert.NoError(t, f.Submit(testNetSSID, testNetPSK))

	assert.NoError(t, waitFor(t, beginErr, "Begin to return"))
	updates := waitFor(t, results, "the update stream to close")
	assert.Equal(t, []iotflow.FlowState{
		iotflow.StateIdle,
		iotflow.StateCheckingConnection,
		iotflow.StateEnablingAP,
		iotflow.StateWaitingForCredentials,
		iotflow.StateDisablingAP,
		iotflow.StateConnecting,
		iotflow.StateAttemptFailed, // wrong PSK; recovering
		iotflow.StateEnablingAP,    // retry loop
		iotflow.StateWaitingForCredentials,
		iotflow.StateDisablingAP,
		iotflow.StateConnecting,
		iotflow.StateProvisioned,
	}, stateSeq(updates))

	// The failure must be attributable: without this a client cannot tell the
	// user their password was wrong, which is the whole point of the state.
	failed, ok := firstWithState(updates, iotflow.StateAttemptFailed)
	assert.True(t, ok, "no StateAttemptFailed update")
	assert.ErrorIs(t, failed.Err, connErr)
	assert.Equal(t, iotflow.ReasonConnectFailed, failed.Reason)
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

	updates := waitFor(t, results, "the update stream to close")
	assert.Equal(t, []iotflow.FlowState{iotflow.StateIdle, iotflow.StateCheckingConnection, iotflow.StateFailed}, stateSeq(updates))
	assert.ErrorIs(t, updates[len(updates)-1].Err, backendErr)
}

func TestBegin_EnableAPModeError(t *testing.T) {
	backendErr := errors.New("radio blocked")
	f := newTestFlow(t, &iotflowtest.MockProvisioner{
		IsConnectedFn:  func(_ context.Context) (bool, error) { return false, nil },
		EnableAPModeFn: func(_ context.Context, _, _ string) (<-chan provision.Update, error) { return nil, backendErr },
	})

	ch, unsub := f.Subscribe()
	defer unsub()

	results, _ := watchUpdates(t, ch, iotflow.StateFailed)

	err := f.Begin(t.Context())
	assert.ErrorIs(t, err, backendErr)

	updates := waitFor(t, results, "the update stream to close")
	assert.Equal(t, []iotflow.FlowState{iotflow.StateIdle, iotflow.StateCheckingConnection, iotflow.StateEnablingAP, iotflow.StateFailed}, stateSeq(updates))
}

// TestBegin_DisableAPModeError tests the flow of state when an error occurs during the DisableAPMode() step
// (e.g. after credentials have been submitted). The failure is not terminal, so the second submission is
// what makes the recovery observable: without it the Flow parks on credsCh and the stream never closes.
func TestBegin_DisableAPModeError(t *testing.T) {
	backendErr := errors.New("cannot deactivate")
	var disableApModeCalls atomic.Int32

	f := newTestFlow(t, &iotflowtest.MockProvisioner{
		IsConnectedFn: func(_ context.Context) (bool, error) { return false, nil },
		EnableAPModeFn: func(_ context.Context, _, _ string) (<-chan provision.Update, error) {
			return iotflowtest.ConnectedCh(), nil
		},
		DisableAPModeFn: func(_ context.Context) error {
			// Return an error for the first call, allow subsequent calls to run.
			if disableApModeCalls.Add(1) == 1 {
				return backendErr
			}
			return nil
		},
		ConnectToNetworkFn: func(_ context.Context, _, _ string) (<-chan provision.Update, error) {
			return iotflowtest.ConnectedCh(), nil
		},
	})

	ch, unsub := f.Subscribe()
	defer unsub()

	allResultsCh, waiting := watchUpdates(t, ch, iotflow.StateWaitingForCredentials)

	beginErr := make(chan error, 1)
	go func() { beginErr <- f.Begin(t.Context()) }()

	waitFor(t, waiting, "StateWaitingForCredentials")
	assert.NoError(t, f.Submit(testNetSSID, testNetPSK))

	// second credential submission due to our first DisableApMode failure
	waitFor(t, waiting, "StateWaitingForCredentials")
	assert.NoError(t, f.Submit(testNetSSID, testNetPSK))

	assert.NoError(t, waitFor(t, beginErr, "Begin to return"))
	allResults := waitFor(t, allResultsCh, "the update stream to close")
	assert.Equal(t, []iotflow.FlowState{
		// Standard flow to set up AP mode
		iotflow.StateIdle,
		iotflow.StateCheckingConnection,
		iotflow.StateEnablingAP,
		iotflow.StateWaitingForCredentials,

		// Accept credentials, but there's an error disabling the AP
		iotflow.StateDisablingAP,
		iotflow.StateAttemptFailed,
		iotflow.StateEnablingAP,
		iotflow.StateWaitingForCredentials,

		// Credentials re-sent by the client, disabling AP mode and connection succeeds.
		iotflow.StateDisablingAP,
		iotflow.StateConnecting,
		iotflow.StateProvisioned,
	}, stateSeq(allResults))

	// assert that the StateAttemptFailed event comes with the error, and is
	// classified as a teardown problem rather than a bad-credentials one.
	failed, ok := firstWithState(allResults, iotflow.StateAttemptFailed)
	assert.True(t, ok, "no StateAttemptFailed update")
	assert.ErrorIs(t, failed.Err, backendErr)
	assert.Equal(t, iotflow.ReasonAPTeardownFailed, failed.Reason)
}

func TestBegin_ContextCancelledWhileWaiting(t *testing.T) {
	f := newTestFlow(t, &iotflowtest.MockProvisioner{
		IsConnectedFn: func(_ context.Context) (bool, error) { return false, nil },
		EnableAPModeFn: func(_ context.Context, _, _ string) (<-chan provision.Update, error) {
			return iotflowtest.ConnectedCh(), nil
		},
	})

	ch, unsub := f.Subscribe()
	defer unsub()

	results, waiting := watchUpdates(t, ch, iotflow.StateWaitingForCredentials)

	ctx, cancel := context.WithCancel(t.Context())
	beginErr := make(chan error, 1)
	go func() { beginErr <- f.Begin(ctx) }()

	waitFor(t, waiting, "StateWaitingForCredentials")
	cancel()

	err := waitFor(t, beginErr, "Begin to return")
	assert.ErrorIs(t, err, context.Canceled)

	updates := waitFor(t, results, "the update stream to close")
	last := updates[len(updates)-1]
	assert.Equal(t, iotflow.StateFailed, last.State)
	assert.ErrorIs(t, last.Err, context.Canceled)
}

// --- Submit ---

// beginWaitingFlow returns a Flow whose Begin is already running and parked in
// StateWaitingForCredentials — the only state in which Submit accepts anything,
// since the state check precedes validation. Begin is cancelled and drained at
// test end.
func beginWaitingFlow(t *testing.T) *iotflow.Flow {
	t.Helper()
	f := newTestFlow(t, &iotflowtest.MockProvisioner{
		IsConnectedFn: func(_ context.Context) (bool, error) { return false, nil },
		EnableAPModeFn: func(_ context.Context, _, _ string) (<-chan provision.Update, error) {
			return iotflowtest.ConnectedCh(), nil
		},
		// Stubbed because an accepted submission lets the machine run on to the
		// connect step; a nil field would panic there.
		ConnectToNetworkFn: func(_ context.Context, _, _ string) (<-chan provision.Update, error) {
			return iotflowtest.ConnectedCh(), nil
		},
	})

	ch, unsub := f.Subscribe()
	t.Cleanup(unsub)
	_, waiting := watchUpdates(t, ch, iotflow.StateWaitingForCredentials)

	ctx, cancel := context.WithCancel(t.Context())
	beginErr := make(chan error, 1)
	go func() { beginErr <- f.Begin(ctx) }()
	t.Cleanup(func() {
		cancel()
		_ = waitFor(t, beginErr, "Begin to return after cancellation")
	})

	waitFor(t, waiting, "StateWaitingForCredentials")
	return f
}

// TestSubmit covers Submit's rejection ladder. Order matters and is asserted
// here: the state check runs first, so a Flow that isn't waiting rejects even
// well-formed credentials, and validation only ever runs on a waiting Flow.
func TestSubmit(t *testing.T) {
	cases := []struct {
		name    string
		waiting bool // drive Begin to StateWaitingForCredentials first
		ssid    string
		psk     string
		wantErr error
	}{
		{
			name:    "accepted while waiting",
			waiting: true,
			ssid:    testNetSSID,
			psk:     testNetPSK,
		},
		{
			// The state check short-circuits before ValidateCredentials, so an
			// idle Flow reports the state, not the (valid) credentials.
			name:    "rejected when not waiting",
			ssid:    testNetSSID,
			psk:     testNetPSK,
			wantErr: iotflow.ErrNotAwaitingCredentials,
		},
		{
			name:    "psk too short",
			waiting: true,
			ssid:    testNetSSID,
			psk:     "short",
			wantErr: provision.ErrPSKInvalid,
		},
		{
			name:    "empty ssid",
			waiting: true,
			ssid:    "",
			psk:     testNetPSK,
			wantErr: provision.ErrSSIDInvalid,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newTestFlow(t, &iotflowtest.MockProvisioner{})
			if tc.waiting {
				f = beginWaitingFlow(t)
			}

			err := f.Submit(tc.ssid, tc.psk)
			if tc.wantErr == nil {
				assert.NoError(t, err)
				return
			}
			assert.ErrorIs(t, err, tc.wantErr)
		})
	}
}

// NOTE: the old TestSubmit_AlreadyPending was removed rather than repaired.
// ErrSubmissionPending is no longer deterministically reachable: in
// StateWaitingForCredentials the state machine is parked on a receive from
// credsCh, so a submission is consumed as soon as it lands and the buffer frees
// up again; in every other state the state check rejects first with
// ErrNotAwaitingCredentials. The error is kept because the window is real for
// concurrent submitters, and the transport's mapping of it stays covered by
// TestCredentialStateStatusCode in transport/http.

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
	assert.Equal(t, iotflow.StateProvisioned, upd.State)

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
	assert.ErrorIs(t, err, provision.ErrSSIDInvalid)

	_, err = iotflow.NewFlow(testNetSSID, "short", &iotflowtest.MockProvisioner{})
	assert.ErrorIs(t, err, provision.ErrPSKInvalid)
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
		EnableAPModeFn: func(_ context.Context, _, _ string) (<-chan provision.Update, error) {
			return iotflowtest.ConnectedCh(), nil
		},
		ConnectToNetworkFn: func(_ context.Context, _, _ string) (<-chan provision.Update, error) {
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

	waitFor(t, waiting, "StateWaitingForCredentials")
	assert.NoError(t, f.Submit(testNetSSID, "wrongpass1"))

	waitFor(t, waiting, "StateWaitingForCredentials")
	assert.NoError(t, f.Submit(testNetSSID, testNetPSK))

	assert.NoError(t, waitFor(t, beginErr, "Begin to return"))
	updates := waitFor(t, results, "the update stream to close")
	assert.Equal(t, []iotflow.FlowState{
		iotflow.StateIdle,
		iotflow.StateCheckingConnection,
		iotflow.StateEnablingAP,
		iotflow.StateWaitingForCredentials,
		iotflow.StateDisablingAP,
		iotflow.StateConnecting,
		iotflow.StateAttemptFailed, // activation failed; recovering
		iotflow.StateEnablingAP,    // retry loop
		iotflow.StateWaitingForCredentials,
		iotflow.StateDisablingAP,
		iotflow.StateConnecting,
		iotflow.StateProvisioned,
	}, stateSeq(updates))

	failed, ok := firstWithState(updates, iotflow.StateAttemptFailed)
	assert.True(t, ok, "no StateAttemptFailed update")
	assert.ErrorIs(t, failed.Err, connErr)
	assert.Equal(t, iotflow.ReasonConnectFailed, failed.Reason)
}

// TestBegin_ContextCancelledDuringConnect covers cancellation on the synchronous
// ConnectToNetwork error path: when ctx is already cancelled, the Flow must fail
// with ctx.Err() rather than looping back into AP mode.
func TestBegin_ContextCancelledDuringConnect(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())

	f := newTestFlow(t, &iotflowtest.MockProvisioner{
		IsConnectedFn: func(_ context.Context) (bool, error) { return false, nil },
		EnableAPModeFn: func(_ context.Context, _, _ string) (<-chan provision.Update, error) {
			return iotflowtest.ConnectedCh(), nil
		},
		ConnectToNetworkFn: func(_ context.Context, _, _ string) (<-chan provision.Update, error) {
			cancel()
			return nil, errors.New("activation failed")
		},
	})

	ch, unsub := f.Subscribe()
	defer unsub()

	results, waiting := watchUpdates(t, ch, iotflow.StateWaitingForCredentials)

	beginErr := make(chan error, 1)
	go func() { beginErr <- f.Begin(ctx) }()

	waitFor(t, waiting, "StateWaitingForCredentials")
	assert.NoError(t, f.Submit(testNetSSID, testNetPSK))

	err := waitFor(t, beginErr, "Begin to return")
	assert.ErrorIs(t, err, context.Canceled)

	updates := waitFor(t, results, "the update stream to close")
	last := updates[len(updates)-1]
	assert.Equal(t, iotflow.StateFailed, last.State)
	assert.ErrorIs(t, last.Err, context.Canceled)
}

// TestBegin_ContextCancelledDuringConnectChannel covers the same cancellation
// guarantee on the channel path: ConnectToNetwork succeeds but its update channel
// never reaches a terminal state, and ctx is cancelled while draining it.
func TestBegin_ContextCancelledDuringConnectChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	blocked := make(chan provision.Update) // never sends, never closes

	f := newTestFlow(t, &iotflowtest.MockProvisioner{
		IsConnectedFn: func(_ context.Context) (bool, error) { return false, nil },
		EnableAPModeFn: func(_ context.Context, _, _ string) (<-chan provision.Update, error) {
			return iotflowtest.ConnectedCh(), nil
		},
		ConnectToNetworkFn: func(_ context.Context, _, _ string) (<-chan provision.Update, error) {
			return blocked, nil
		},
	})

	ch, unsub := f.Subscribe()
	defer unsub()

	results, connecting := watchUpdates(t, ch, iotflow.StateConnecting)

	// A second subscription tracks the earlier state: Submit is rejected unless
	// the Flow has reached StateWaitingForCredentials, and a rejected submission
	// leaves the machine parked there forever — so the connect path under test
	// is never entered.
	credsCh, unsubCreds := f.Subscribe()
	defer unsubCreds()
	_, waitingForCreds := watchUpdates(t, credsCh, iotflow.StateWaitingForCredentials)

	beginErr := make(chan error, 1)
	go func() { beginErr <- f.Begin(ctx) }()

	waitFor(t, waitingForCreds, "StateWaitingForCredentials")
	assert.NoError(t, f.Submit(testNetSSID, testNetPSK))

	waitFor(t, connecting, "StateConnecting") // Flow is now blocked draining the connect channel
	cancel()

	err := waitFor(t, beginErr, "Begin to return")
	assert.ErrorIs(t, err, context.Canceled)

	updates := waitFor(t, results, "the update stream to close")
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
		EnableAPModeFn: func(_ context.Context, _, _ string) (<-chan provision.Update, error) {
			return iotflowtest.ConnectedCh(), nil
		},
		ConnectToNetworkFn: func(_ context.Context, ssid, _ string) (<-chan provision.Update, error) {
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

	waitFor(t, waiting, "StateWaitingForCredentials")
	assert.NoError(t, f.Submit(testNetSSID, testNetPSK))

	// Second wait: by now the retry has run drainCreds, so the stale submission
	// is gone and the Flow is blocked waiting for a fresh credential.
	waitFor(t, waiting, "StateWaitingForCredentials")
	assert.NoError(t, f.Submit("fresh-net", "freshpassword"))

	assert.NoError(t, waitFor(t, beginErr, "Begin to return"))
	assert.Equal(t, "fresh-net", secondConnectSSID,
		"retry must use the freshly submitted credential, not the stale mid-connect one")
}

// --- Wire encoding ---

// TestFlowUpdateMarshalJSONRedactsError pins the boundary between what the Flow
// knows and what a client is told. Err holds the backend's own error text —
// D-Bus object paths, interface names, NetworkManager internals — and the far
// end of the SSE stream is an unauthenticated client sitting on the setup AP.
// The encoded form must carry the classification instead, never the raw text.
func TestFlowUpdateMarshalJSONRedactsError(t *testing.T) {
	// A realistically leaky backend error.
	rawErr := errors.New(`dbus: org.freedesktop.NetworkManager.Device.Error: ` +
		`/org/freedesktop/NetworkManager/Devices/3 refused Deactivate for uuid 9f2c`)

	tests := []struct {
		name       string
		update     iotflow.FlowUpdate
		wantState  string
		wantReason string
		wantErrMsg string
	}{
		{
			name:      "clean update carries no error fields",
			update:    iotflow.FlowUpdate{State: iotflow.StateWaitingForCredentials},
			wantState: "WaitingForCredentials",
		},
		{
			name: "connect failure is attributable",
			update: iotflow.FlowUpdate{
				State: iotflow.StateAttemptFailed, Err: rawErr, Reason: iotflow.ReasonConnectFailed,
			},
			wantState:  "AttemptFailed",
			wantReason: "connect_failed",
			wantErrMsg: iotflow.ReasonConnectFailed.Message(),
		},
		{
			name: "teardown failure is distinguishable from a bad password",
			update: iotflow.FlowUpdate{
				State: iotflow.StateAttemptFailed, Err: rawErr, Reason: iotflow.ReasonAPTeardownFailed,
			},
			wantState:  "AttemptFailed",
			wantReason: "ap_teardown_failed",
			wantErrMsg: iotflow.ReasonAPTeardownFailed.Message(),
		},
		{
			name:       "unclassified error still reports generically",
			update:     iotflow.FlowUpdate{State: iotflow.StateFailed, Err: rawErr},
			wantState:  "Failed",
			wantReason: "internal",
			wantErrMsg: iotflow.ReasonInternal.Message(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := json.Marshal(tt.update)
			assert.NoError(t, err)

			var got struct {
				State  string `json:"state"`
				Reason string `json:"reason"`
				Error  string `json:"error"`
			}
			assert.NoError(t, json.Unmarshal(b, &got))
			assert.Equal(t, tt.wantState, got.State)
			assert.Equal(t, tt.wantReason, got.Reason)
			assert.Equal(t, tt.wantErrMsg, got.Error)

			// The payload must not contain the backend's text under any key.
			assert.NotContains(t, string(b), "dbus")
			assert.NotContains(t, string(b), "NetworkManager")
			assert.NotContains(t, string(b), "9f2c")
		})
	}
}

// TestFlowStateStringRoundTrip guards the String() switch. A missing case falls
// through to "Unknown", which reaches the wire silently — every state the Flow
// can emit must render as itself.
func TestFlowStateStringRoundTrip(t *testing.T) {
	states := []iotflow.FlowState{
		iotflow.StateIdle, iotflow.StateCheckingConnection, iotflow.StateEnablingAP,
		iotflow.StateWaitingForCredentials, iotflow.StateDisablingAP, iotflow.StateConnecting,
		iotflow.StateProvisioned, iotflow.StateFailed, iotflow.StateAttemptFailed,
	}
	seen := make(map[string]iotflow.FlowState, len(states))
	for _, s := range states {
		got := s.String()
		assert.NotEqual(t, "Unknown", got, "state %d has no String() case", s)
		if prev, dup := seen[got]; dup {
			t.Fatalf("states %d and %d both render as %q", prev, s, got)
		}
		seen[got] = s
	}
}

// TestSubscribeReplaysUnresolvedFailure covers the subscriber that arrives
// *after* a recovery. StateAttemptFailed is emit-only, so it is overwritten in
// lastStateUpdate almost immediately — and this is the normal case rather than
// an edge case, because re-entering AP mode bounces the radio the client is
// connected over, dropping and reconnecting its stream. Without a replay such a
// client is returned to the setup page with no idea why.
func TestSubscribeReplaysUnresolvedFailure(t *testing.T) {
	connErr := errors.New("authentication failed")
	var callCount atomic.Int32
	secondAttempt := make(chan struct{})

	f := newTestFlow(t, &iotflowtest.MockProvisioner{
		IsConnectedFn: func(_ context.Context) (bool, error) { return false, nil },
		EnableAPModeFn: func(_ context.Context, _, _ string) (<-chan provision.Update, error) {
			return iotflowtest.ConnectedCh(), nil
		},
		ConnectToNetworkFn: func(_ context.Context, _, _ string) (<-chan provision.Update, error) {
			if callCount.Add(1) == 1 {
				return iotflowtest.FailedCh(connErr), nil
			}
			<-secondAttempt // hold the Flow so the assertions below are stable
			return iotflowtest.ConnectedCh(), nil
		},
	})

	first, unsubFirst := f.Subscribe()
	_, waiting := watchUpdates(t, first, iotflow.StateWaitingForCredentials)

	beginErr := make(chan error, 1)
	go func() { beginErr <- f.Begin(t.Context()) }()

	waitFor(t, waiting, "StateWaitingForCredentials")
	assert.NoError(t, f.Submit(testNetSSID, "wrongpass1"))

	// The connect fails and the Flow recovers to StateWaitingForCredentials.
	waitFor(t, waiting, "StateWaitingForCredentials after the failure")
	unsubFirst() // the client's stream drops when the AP bounces

	// A fresh subscription, standing in for the browser's SSE reconnect.
	second, unsubSecond := f.Subscribe()
	defer unsubSecond()

	failed := waitFor(t, second, "the replayed failure")
	assert.Equal(t, iotflow.StateAttemptFailed, failed.State,
		"a reconnecting client must be told why it is being asked again")
	assert.ErrorIs(t, failed.Err, connErr)
	assert.Equal(t, iotflow.ReasonConnectFailed, failed.Reason)

	current := waitFor(t, second, "the current state")
	assert.Equal(t, iotflow.StateWaitingForCredentials, current.State,
		"the replayed failure must be followed by the live state, in emit order")

	// Once provisioning succeeds the failure is resolved and must not be
	// replayed to anyone joining afterwards.
	assert.NoError(t, f.Submit(testNetSSID, testNetPSK))
	close(secondAttempt)
	assert.NoError(t, waitFor(t, beginErr, "Begin to return"))

	third, _ := f.Subscribe()
	last := waitFor(t, third, "the terminal state")
	assert.Equal(t, iotflow.StateProvisioned, last.State,
		"a subscriber joining after success must not be shown a stale failure")
}
