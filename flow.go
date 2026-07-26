package iotflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/dawsonalex/iotflow/provision"
)

// FlowState represents the current state of a provisioning Flow.
type FlowState uint8

const (
	StateIdle                  FlowState = iota // zero value; Flow has not started
	StateCheckingConnection                     // IsConnected in progress
	StateEnablingAP                             // EnableAPMode in progress
	StateWaitingForCredentials                  // AP is up; waiting for a credential submission
	StateDisablingAP                            // DisableAPMode in progress
	StateConnecting                             // ConnectToNetwork in progress
	StateProvisioned                            // terminal: device is on a network, whether it was already connected at Begin or completed the flow
	StateFailed                                 // terminal: unrecoverable error
	StateAttemptFailed                          // non-terminal: an attempt failed recoverably (cause in Err/Reason) and the Flow re-enters AP mode. Emit-only — the machine never dispatches on it, the next update names the state it moved to.
)

func (s FlowState) String() string {
	switch s {
	case StateIdle:
		return "Idle"
	case StateCheckingConnection:
		return "CheckingConnection"
	case StateEnablingAP:
		return "EnablingAP"
	case StateWaitingForCredentials:
		return "WaitingForCredentials"
	case StateDisablingAP:
		return "DisablingAP"
	case StateConnecting:
		return "Connecting"
	case StateProvisioned:
		return "Provisioned"
	case StateFailed:
		return "Failed"
	case StateAttemptFailed:
		return "AttemptFailed"
	default:
		return "Unknown"
	}
}

// FailureReason classifies why an attempt failed, in terms a client can act on
// without being handed the backend's error text. Err carries the detail for
// logs and for Go consumers; Reason is what is safe to put on the wire.
type FailureReason uint8

const (
	ReasonNone             FailureReason = iota // no failure
	ReasonConnectFailed                         // ConnectToNetwork failed; the credentials or the target network are suspect
	ReasonAPTeardownFailed                      // DisableAPMode failed; a device-side problem, resubmitting is still worth a try
	ReasonInternal                              // anything else
)

func (r FailureReason) String() string {
	switch r {
	case ReasonNone:
		return "none"
	case ReasonConnectFailed:
		return "connect_failed"
	case ReasonAPTeardownFailed:
		return "ap_teardown_failed"
	case ReasonInternal:
		return "internal"
	default:
		return "unknown"
	}
}

// Message returns a client-facing description of the reason, safe to display to
// whoever is holding the setup page. It deliberately says nothing about the
// backend: transports use this in place of Err.Error() so that D-Bus object
// paths, interface names and other internals do not reach an unauthenticated
// client sitting on the provisioning AP.
func (r FailureReason) Message() string {
	switch r {
	case ReasonNone:
		return ""
	case ReasonConnectFailed:
		return "could not join the network — check the name and password and try again"
	case ReasonAPTeardownFailed:
		return "the device could not switch out of setup mode — please try again"
	default:
		return "the device hit an internal error"
	}
}

// FlowUpdate carries a state transition and any associated error.
//
// Err is the raw backend error. It is useful to a Go consumer (errors.Is still
// works) but it is not safe to hand to a client: see Reason and MarshalJSON.
type FlowUpdate struct {
	State  FlowState
	Err    error         // non-nil when State is StateFailed or StateAttemptFailed
	Reason FailureReason // classification of Err; ReasonNone when Err is nil
}

// MarshalJSON renders a FlowUpdate for the wire. The bare struct is not directly
// serializable: State would encode as an opaque integer and Err (an interface)
// as null, so transports streaming updates (e.g. SSE) get a stable shape here.
//
// The encoding is deliberately redacted: it carries Reason and Reason.Message(),
// never Err.Error(). Marshalling a FlowUpdate means putting it on a wire, and
// the far end of that wire is an unauthenticated client on the provisioning AP —
// handing it raw backend errors leaks D-Bus object paths, interface names and
// other internals, and invites clients to string-match on them. Consumers that
// need the underlying error read Err directly from the struct; errors.Is still
// works there.
func (u FlowUpdate) MarshalJSON() ([]byte, error) {
	msg := ""
	reason := ""
	if u.Err != nil {
		r := u.Reason
		if r == ReasonNone {
			// An error with no classification is still an error; say so
			// generically rather than dropping it from the payload.
			r = ReasonInternal
		}
		msg = r.Message()
		reason = r.String()
	}
	return json.Marshal(struct {
		State  string `json:"state"`
		Reason string `json:"reason,omitempty"`
		Error  string `json:"error,omitempty"`
	}{
		State:  u.State.String(),
		Reason: reason,
		Error:  msg,
	})
}

type credentials struct {
	ssid string
	psk  string
}

var (
	// ErrSubmissionPending is returned by Submit when a previous submission has not
	// yet been consumed by the state machine.
	ErrSubmissionPending = errors.New("a credential submission is already pending")

	// ErrNotAwaitingCredentials is returned by Submit when the Flow is not in a state to
	// accept new credentials.
	ErrNotAwaitingCredentials = errors.New("not in StateWaitingForCredentials")

	// ErrUpdateChanClosedPrematurely is returned when a flow state channel was closed, but the flow did not reach
	// a terminal state.
	ErrUpdateChanClosedPrematurely = errors.New("update channel closed before reaching a terminal state")
)

// Flow orchestrates the full WiFi provisioning lifecycle: checking connection
// status, enabling AP mode, receiving station credentials, disabling AP mode,
// and connecting to the target network. A DisableAPMode or ConnectToNetwork
// failure is not terminal: the Flow emits StateAttemptFailed with the cause and
// a FailureReason, then re-enters AP mode so the user can submit new
// credentials. Each retry is gated on a fresh submission, so the loop cannot
// spin.
//
// Flow is transport-agnostic. Credentials arrive via Submit and progress is
// observed via Subscribe; a transport (such as the httphandler subpackage)
// adapts those two seams to HTTP, BLE, MQTT, etc. The Flow itself knows nothing
// about how credentials reach it or how updates are delivered.
type Flow struct {
	provisioner provision.Provisioner
	apSSID      string
	apPSK       string

	credsCh chan credentials // Submit → state machine

	mu              sync.Mutex                   // guards subs, closed, lastStateUpdate, lastFailure
	subs            map[chan FlowUpdate]struct{} // active update subscribers
	closed          bool                         // true once Begin has returned
	lastStateUpdate FlowUpdate                   // most recent emitted update; replayed to new subscribers.

	// lastFailure is the most recent StateAttemptFailed update, retained until
	// the Flow reaches StateProvisioned. StateAttemptFailed is emit-only, so it
	// is overwritten in lastStateUpdate almost immediately and a subscriber that
	// arrives afterwards would never learn why it is being asked for credentials
	// again. That subscriber is the normal case, not an edge case: re-entering
	// AP mode bounces the radio the client is connected over, so an SSE stream
	// drops and reconnects on exactly this path.
	lastFailure *FlowUpdate
}

type FlowOpt func(*Flow)

// NewFlow creates a new flow for Provisioner p. apSsid and apPsk set ssid and password for
// the flows AP mode.
// Flow.Finish() should be called when provisioning is done so that resources can be released.
func NewFlow(apSsid, apPsk string, p provision.Provisioner, opts ...FlowOpt) (*Flow, error) {
	if err := provision.ValidateCredentials(apSsid, apPsk); err != nil {
		return nil, err
	}

	f := &Flow{
		apSSID:      apSsid,
		apPSK:       apPsk,
		provisioner: p,
		credsCh:     make(chan credentials, 1),
		subs:        make(map[chan FlowUpdate]struct{}),
	}

	for _, o := range opts {
		o(f)
	}
	return f, nil
}

// Submit hands a set of station credentials to the running Flow. It is
// non-blocking and safe for concurrent use. Credentials are validated before
// being accepted; an invalid SSID or PSK returns ErrSSIDInvalid/ErrPSKInvalid.
// If a previous submission is still pending, Submit returns ErrSubmissionPending.
//
// If the Flow is not in a state to accept new credentials, Submit returns ErrNotAwaitingCredentials
// and the credentials are discarded.
func (f *Flow) Submit(ssid, psk string) error {
	// lastStateUpdate is written by emit under mu, so the read must be guarded.
	// The lock is released before the send: the state can only be a snapshot
	// anyway (the machine may move on the instant it is read), and holding mu
	// across the channel send would deadlock against emit.
	f.mu.Lock()
	state := f.lastStateUpdate.State
	f.mu.Unlock()

	if state != StateWaitingForCredentials {
		return ErrNotAwaitingCredentials
	}

	if err := provision.ValidateCredentials(ssid, psk); err != nil {
		return err
	}
	select {
	case f.credsCh <- credentials{ssid: ssid, psk: psk}:
		return nil
	default:
		return ErrSubmissionPending
	}
}

// Subscribe returns a channel that receives a FlowUpdate on each state
// transition. Multiple subscribers may observe concurrently. The channel is
// buffered; if a subscriber falls behind, updates are dropped rather than
// blocking the state machine. Every channel is closed when Begin returns.
//
// The first event down the returned channel is always the current state of the flow.
//
// Call the returned function to release a subscription early (e.g. when an SSE client
// disconnects); otherwise it is released when Begin returns.
func (f *Flow) Subscribe() (<-chan FlowUpdate, func()) {
	f.mu.Lock()
	defer f.mu.Unlock()

	noopUnsub := func() {}

	ch := make(chan FlowUpdate, 16)
	// Replay the current state into the buffer. The channel is freshly created
	// with spare capacity, so these sends never block. Holding mu makes the
	// replay-and-register atomic with emit: a subscriber can neither miss a
	// transition nor receive the current state twice.
	//
	// An unresolved failure is replayed first, in the same order it was emitted
	// live, so a client that connected after the recovery still learns why.
	if f.lastFailure != nil {
		ch <- *f.lastFailure
	}
	ch <- f.lastStateUpdate
	if f.closed {
		close(ch)
		return ch, noopUnsub
	}

	f.subs[ch] = struct{}{}
	return ch, func() {
		f.removeSub(ch)
	}
}

func (f *Flow) removeSub(ch chan FlowUpdate) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.subs[ch]; ok {
		close(ch)
	}
	delete(f.subs, ch)
}

// Begin runs the provisioning lifecycle, blocking until the device is
// connected or an unrecoverable error occurs. ctx cancellation stops the flow
// and returns ctx.Err(). Begin must only be called once per Flow.
func (f *Flow) Begin(ctx context.Context) error {
	defer f.closeSubs()
	// TODO: Make subsequent Begin calls idempotent.

	var pendingCreds credentials

	state := StateCheckingConnection
	f.emit(FlowUpdate{State: state})

	for {
		switch state {
		case StateCheckingConnection:
			connected, err := f.provisioner.IsConnected(ctx)
			if err != nil {
				return f.fail(err)
			}
			if connected {
				// Already on a network: the device is provisioned, it just
				// didn't need this run to get there. Subscribers see the same
				// terminal state either way.
				f.emit(FlowUpdate{State: StateProvisioned})
				return nil
			}
			state = StateEnablingAP
			f.emit(FlowUpdate{State: state})

		case StateEnablingAP:
			updates, err := f.provisioner.EnableAPMode(ctx, f.apSSID, f.apPSK)
			if err != nil {
				return f.fail(err)
			}
			if err := drainUntilDone(ctx, updates); err != nil {
				return f.fail(err)
			}
			state = StateWaitingForCredentials
			f.emit(FlowUpdate{State: state})

		case StateWaitingForCredentials:
			select {
			case <-ctx.Done():
				return f.fail(ctx.Err())
			case creds := <-f.credsCh:
				pendingCreds = creds
				state = StateDisablingAP
				f.emit(FlowUpdate{State: state})
			}

		case StateDisablingAP:
			if err := f.provisioner.DisableAPMode(ctx); err != nil {
				// Not terminal: the AP is most likely still up, so going terminal
				// here would strand the user on it with no way to resubmit.
				// Re-entering AP mode reconverges whether the AP survived or was
				// partially torn down.
				state = f.retry(StateEnablingAP, err, ReasonAPTeardownFailed)
				continue
			}
			state = StateConnecting
			f.emit(FlowUpdate{State: state})

		case StateConnecting:
			updates, err := f.provisioner.ConnectToNetwork(ctx, pendingCreds.ssid, pendingCreds.psk)
			if err != nil {
				if ctx.Err() != nil {
					return f.fail(ctx.Err())
				}
				// Retry: re-enable AP so the user can submit new credentials.
				state = f.retry(StateEnablingAP, err, ReasonConnectFailed)
				continue
			}
			if err := drainUntilDone(ctx, updates); err != nil {
				if ctx.Err() != nil {
					return f.fail(ctx.Err())
				}
				// Connection failed — loop back so the user can try again.
				state = f.retry(StateEnablingAP, err, ReasonConnectFailed)
				continue
			}
			f.emit(FlowUpdate{State: StateProvisioned})
			return nil
		}
	}
}

// Finish releases the Flow's resources
func (f *Flow) Finish() error {
	f.closeSubs()

	// TODO: It might be worth having this function cleanup the flow state so that Flow.Finish() -> Flow.Begin()
	// 	doesn't leave any stale state behind.

	return nil
}

func (f *Flow) ListAccessPoints(ctx context.Context) ([]provision.Network, error) {
	aps, err := f.provisioner.Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("flow: listing access points: %w", err)
	}
	return aps, nil
}

// emit fans an update out to all subscribers without blocking the state
// machine. A subscriber whose buffer is full misses the update.
func (f *Flow) emit(u FlowUpdate) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastStateUpdate = u

	switch u.State {
	case StateAttemptFailed:
		// Retain it for subscribers that arrive after the recovery.
		upd := u
		f.lastFailure = &upd
	case StateProvisioned:
		// The failure is resolved; a late subscriber should not be told the
		// device had trouble when it is now on the network.
		f.lastFailure = nil
	}

	for c := range f.subs {
		select {
		case c <- u:
		default:
		}
	}
}

func (f *Flow) fail(err error) error {
	f.emit(FlowUpdate{State: StateFailed, Err: err, Reason: ReasonInternal})
	return err
}

// retry announces a recoverable failure and hands the machine to next. The
// StateAttemptFailed update is what lets a client say *why* the setup page came
// back; the update after it names the state actually entered, since
// StateAttemptFailed is never occupied. Returns next so call sites read as a
// single assignment.
func (f *Flow) retry(next FlowState, err error, reason FailureReason) FlowState {
	f.emit(FlowUpdate{State: StateAttemptFailed, Err: err, Reason: reason})
	f.drainCreds()
	f.emit(FlowUpdate{State: next})
	return next
}

// closeSubs marks the Flow finished and closes every active subscription. After
// this, Subscribe returns already-closed channels.
func (f *Flow) closeSubs() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	for c := range f.subs {
		close(c)
		delete(f.subs, c)
	}
}

// drainCreds discards any buffered submission. Called before re-entering AP
// mode on a retry so a credential that arrived mid-connect is not silently
// reused in place of a fresh one.
func (f *Flow) drainCreds() {
	select {
	case <-f.credsCh:
	default:
	}
}

// drainUntilDone reads from ch until a terminal ProvisionState is reached or
// ctx is cancelled. Returns nil on ProvisionStateConnected, an error otherwise.
// If ch closes without a terminal state (because the poller saw ctx.Done),
// returns ctx.Err() if the context is cancelled, or ErrUpdateChanClosedPrematurely
// if the update channel is closed before a terminal state is reached.
func drainUntilDone(ctx context.Context, ch <-chan provision.Update) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case upd, ok := <-ch:
			if !ok {
				if err := ctx.Err(); err != nil {
					return err
				}
				return ErrUpdateChanClosedPrematurely
			}
			if upd.State == provision.StateFailed {
				return upd.Err
			}
			if upd.State == provision.StateConnected {
				return nil
			}
		}
	}
}
