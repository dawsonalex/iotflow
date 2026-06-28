package iotflow

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
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
	StateConnected                              // terminal: device was already connected at Begin
	StateProvisioned                            // terminal: device completed the full provisioning flow
	StateFailed                                 // terminal: unrecoverable error
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
	case StateConnected:
		return "Connected"
	case StateProvisioned:
		return "Provisioned"
	case StateFailed:
		return "Failed"
	default:
		return "Unknown"
	}
}

// FlowUpdate carries a state transition and any associated error.
type FlowUpdate struct {
	State FlowState
	Err   error // non-nil only when State == StateFailed
}

// MarshalJSON renders a FlowUpdate for the wire. The bare struct is not directly
// serializable: State would encode as an opaque integer and Err (an interface)
// as null, so transports streaming updates (e.g. SSE) get a stable shape here.
func (u FlowUpdate) MarshalJSON() ([]byte, error) {
	errStr := ""
	if u.Err != nil {
		errStr = u.Err.Error()
	}
	return json.Marshal(struct {
		State string `json:"state"`
		Error string `json:"error,omitempty"`
	}{
		State: u.State.String(),
		Error: errStr,
	})
}

type credentials struct {
	ssid string
	psk  string
}

// ErrSubmissionPending is returned by Submit when a previous submission has not
// yet been consumed by the state machine.
var ErrSubmissionPending = errors.New("a credential submission is already pending")

// Flow orchestrates the full WiFi provisioning lifecycle: checking connection
// status, enabling AP mode, receiving station credentials, disabling AP mode,
// and connecting to the target network. If ConnectToNetwork fails the Flow
// re-enters AP mode so the user can submit new credentials.
//
// Flow is transport-agnostic. Credentials arrive via Submit and progress is
// observed via Subscribe; a transport (such as the httphandler subpackage)
// adapts those two seams to HTTP, BLE, MQTT, etc. The Flow itself knows nothing
// about how credentials reach it or how updates are delivered.
type Flow struct {
	provisioner Provisioner
	apSSID      string
	apPSK       string

	credsCh chan credentials // Submit → state machine

	mu     sync.Mutex                   // guards subs and closed
	subs   map[chan FlowUpdate]struct{} // active update subscribers
	closed bool                         // true once Begin has returned
}

//NewFlow returns a Flow that provisions through p. apSSID and apPSK are the
//credentials for the device's own AP, validated up front.
//func NewFlow(p Provisioner, apSSID, apPSK string) (*Flow, error) {
//	if err := validateCredentials(apSSID, apPSK); err != nil {
//		return nil, err
//	}
//	return &Flow{
//		provisioner: p,
//		apSSID:      apSSID,
//		apPSK:       apPSK,
//		credsCh:     make(chan credentials, 1),
//		subs:        make(map[chan FlowUpdate]struct{}),
//	}, nil
//}

type FlowOpt func(*Flow)

func NewFlow(p Provisioner, opts ...FlowOpt) (*Flow, error) {
	f := &Flow{
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
func (f *Flow) Submit(ssid, psk string) error {
	if err := validateCredentials(ssid, psk); err != nil {
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
// Subscribing after Begin has returned yields an already-closed channel.
//
// Call Unsubscribe to release a subscription early (e.g. when an SSE client
// disconnects); otherwise it is released when Begin returns.
func (f *Flow) Subscribe() <-chan FlowUpdate {
	f.mu.Lock()
	defer f.mu.Unlock()

	ch := make(chan FlowUpdate, 16)
	if f.closed {
		close(ch)
		return ch
	}
	f.subs[ch] = struct{}{}
	return ch
}

// Unsubscribe removes and closes a subscription previously returned by
// Subscribe. It is a no-op if the channel is unknown or already released.
func (f *Flow) Unsubscribe(ch <-chan FlowUpdate) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for c := range f.subs {
		if c == ch {
			delete(f.subs, c)
			close(c)
			return
		}
	}
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
				f.emit(FlowUpdate{State: StateConnected})
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
			if err := f.provisioner.DisableAPMode(); err != nil {
				return f.fail(err)
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
				f.drainCreds()
				state = StateEnablingAP
				f.emit(FlowUpdate{State: state})
				continue
			}
			if err := drainUntilDone(ctx, updates); err != nil {
				if ctx.Err() != nil {
					return f.fail(ctx.Err())
				}
				// Connection failed — loop back so the user can try again.
				f.drainCreds()
				state = StateEnablingAP
				f.emit(FlowUpdate{State: state})
				continue
			}
			f.emit(FlowUpdate{State: StateProvisioned})
			return nil
		}
	}
}

func (f *Flow) Finish() error {
	// TODO: Probably need to also close other things here (server, etc)
	return f.provisioner.Close()
}

// emit fans an update out to all subscribers without blocking the state
// machine. A subscriber whose buffer is full misses the update.
func (f *Flow) emit(u FlowUpdate) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for c := range f.subs {
		select {
		case c <- u:
		default:
		}
	}
}

func (f *Flow) fail(err error) error {
	f.emit(FlowUpdate{State: StateFailed, Err: err})
	return err
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
// returns ctx.Err() so the caller can distinguish cancellation from a clean close.
func drainUntilDone(ctx context.Context, ch <-chan ProvisionUpdate) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case upd, ok := <-ch:
			if !ok {
				return ctx.Err()
			}
			if upd.State == ProvisionStateFailed {
				return upd.Err
			}
			if upd.State == ProvisionStateConnected {
				return nil
			}
		}
	}
}
