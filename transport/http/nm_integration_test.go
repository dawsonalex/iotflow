//go:build integration

// This file is the transport-layer half of the integration suite. Where the
// root package's nm_integration_test.go drives the provisioner directly, these
// tests drive the *whole* stack — HTTP handler -> Flow state machine ->
// NetworkManagerProvisioner -> D-Bus -> fake NetworkManager — exactly as a real
// captive-portal client would.
//
// Like the root suite it relies on the DBUS_SYSTEM_BUS_ADDRESS seam (see
// internal/nmfake): TestMain points that variable at a private bus owned by the
// fake, so the unmodified production Flow/provisioner talk to the fake as though
// it were the system NetworkManager.
//
// Run with: go test -tags integration -race ./...  (needs dbus-daemon on PATH).
package http

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dawsonalex/iotflow"
	"github.com/dawsonalex/iotflow/internal/nmfake"
	"github.com/dawsonalex/iotflow/provision"
	"github.com/dawsonalex/iotflow/provision/networkmanager"
)

// fake is the shared fake NetworkManager for this package's integration tests.
// One bus and one fake serve the whole package because godbus's SystemBus()
// caches its connection process-wide; each test calls fake.Reset() first.
var fake *nmfake.NM

func TestMain(m *testing.M) {
	addr, stopBus, err := nmfake.StartBus()
	if err != nil {
		fmt.Fprintln(os.Stderr, "nmfake.StartBus:", err)
		os.Exit(1)
	}

	f, stopNM, err := nmfake.Start(addr)
	if err != nil {
		stopBus()
		fmt.Fprintln(os.Stderr, "nmfake.Start:", err)
		os.Exit(1)
	}
	fake = f

	if err := os.Setenv("DBUS_SYSTEM_BUS_ADDRESS", addr); err != nil {
		stopNM()
		stopBus()
		fmt.Fprintln(os.Stderr, "setting DBUS_SYSTEM_BUS_ADDRESS:", err)
		os.Exit(1)
	}

	code := m.Run()

	stopNM()
	stopBus()
	os.Exit(code)
}

// newFlowServer builds a real NetworkManager-backed Flow for iface, mounts the
// production HTTP handler in front of it, and returns a running test server. The
// Flow owns its provisioner; both are torn down (which closes and lets the next
// test transparently reconnect the shared system bus) at test end.
func newFlowServer(t *testing.T, iface string) (*httptest.Server, *iotflow.Flow) {
	t.Helper()
	nmProvisioner, err := networkmanager.NewProvisioner(iface)
	if err != nil {
		t.Fatalf("NewNetworkManagerProvisioner(%q): %v", iface, err)
	}
	t.Cleanup(func() { _ = nmProvisioner.Close() })

	flow, err := iotflow.NewFlow("iot-setup", "ap-password", nmProvisioner)
	if err != nil {
		t.Fatalf("NewNetworkManagerFlow(%q): %v", iface, err)
	}
	t.Cleanup(func() { _ = flow.Finish() })

	srv := httptest.NewServer(NewHandler(flow))
	t.Cleanup(srv.Close)
	return srv, flow
}

// sseEvent mirrors the JSON that FlowUpdate.MarshalJSON emits onto the wire.
type sseEvent struct {
	State  string `json:"state"`
	Reason string `json:"reason"`
	Error  string `json:"error"`
}

// openEventStream connects to GET {base}/events and streams decoded events. The
// GET has returned (response headers received) before this function does, which
// guarantees the Flow subscription is already registered — so no state emitted
// after the call can be missed. Call the returned func to disconnect.
func openEventStream(t *testing.T, base string) (<-chan sseEvent, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/events", nil)
	if err != nil {
		cancel()
		t.Fatalf("building events request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		cancel()
		t.Fatalf("connecting to events stream: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		cancel()
		t.Fatalf("GET /events = %d, want 200", resp.StatusCode)
	}

	out := make(chan sseEvent, 32)
	go func() {
		defer close(out)
		defer func() { _ = resp.Body.Close() }()
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			data, ok := strings.CutPrefix(sc.Text(), "data: ")
			if !ok {
				continue // blank separators and ": keepalive" comments
			}
			var ev sseEvent
			if err := json.Unmarshal([]byte(data), &ev); err != nil {
				continue
			}
			select {
			case out <- ev:
			case <-ctx.Done():
				return
			}
		}
	}()

	return out, cancel
}

// waitForState consumes events until it sees want. Because Flow emits states in
// order, waiting for a later state transparently skips the intermediate ones.
func waitForState(t *testing.T, events <-chan sseEvent, want iotflow.FlowState, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	var seen []string
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatalf("event stream closed before reaching %q; saw %v", want, seen)
			}
			seen = append(seen, ev.State)
			if ev.State == want.String() {
				return
			}
		case <-deadline:
			t.Fatalf("timed out waiting for state %q; saw %v", want, seen)
		}
	}
}

// postCredentials POSTs a credential submission, asserts the response status,
// and returns the decoded error body. On a 202 there is no body, so the zero
// value comes back; callers that only care about the status ignore the result.
func postCredentials(t *testing.T, base, ssid, psk string, wantStatus int) errorResponse {
	t.Helper()
	body, err := json.Marshal(credentialsRequest{SSID: ssid, PSK: psk})
	if err != nil {
		t.Fatalf("marshaling credentials: %v", err)
	}
	resp, err := http.Post(base+"/credentials", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /credentials: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != wantStatus {
		t.Fatalf("POST /credentials(%q) = %d, want %d", ssid, resp.StatusCode, wantStatus)
	}
	if resp.StatusCode == http.StatusAccepted {
		return errorResponse{}
	}

	var errRes errorResponse
	if err := json.NewDecoder(resp.Body).Decode(&errRes); err != nil {
		t.Fatalf("decoding error body of POST /credentials(%q): %v", ssid, err)
	}
	return errRes
}

// TestProvisioningRetryThenSuccess drives the full provisioning lifecycle over
// HTTP: the device brings up its AP, the client submits credentials that fail to
// connect, the Flow re-enters AP mode, the client submits working credentials,
// and the device reaches the terminal Provisioned state — all observed through
// the SSE stream and driven through POST /credentials.
func TestProvisioningRetryThenSuccess(t *testing.T) {
	fake.Reset()
	fake.AddWiFiDevice("wlan0")
	// AP mode always comes up (default apScript). The first station connect must
	// fail so the Flow exercises its AP re-entry / retry path.
	fake.SetActivateOutcome(nmfake.FailScript()...)

	srv, flow := newFlowServer(t, "wlan0")

	events, closeStream := openEventStream(t, srv.URL)
	defer closeStream()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	beginErr := make(chan error, 1)
	go func() { beginErr <- flow.Begin(ctx) }()

	// AP is up; the portal is waiting for the user's network credentials.
	waitForState(t, events, iotflow.StateWaitingForCredentials, 5*time.Second)

	// Submit credentials that are well-formed but fail to associate (wrong PSK).
	postCredentials(t, srv.URL, "home-net", "wrong-password", http.StatusAccepted)

	// The connect fails, so the Flow loops back into AP mode and waits again.
	waitForState(t, events, iotflow.StateEnablingAP, 5*time.Second)
	waitForState(t, events, iotflow.StateWaitingForCredentials, 5*time.Second)

	// Let the next station connect succeed, then submit working credentials.
	fake.SetActivateOutcome(nmfake.ConnectScript()...)
	postCredentials(t, srv.URL, "home-net", "right-password", http.StatusAccepted)

	// The device completes provisioning.
	waitForState(t, events, iotflow.StateProvisioned, 5*time.Second)

	select {
	case err := <-beginErr:
		if err != nil {
			t.Fatalf("Begin returned %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Begin did not return after provisioning completed")
	}

	// Four activations in all: AP up, station (fail), AP up again, station (ok).
	if got := fake.AddActivateCalls(); got != 4 {
		t.Fatalf("AddAndActivateConnection called %d times, want 4 (AP, station-fail, AP, station-ok)", got)
	}
	// AP mode was torn down before each station attempt.
	if got := fake.DeactivateCalls(); got != 2 {
		t.Fatalf("DeactivateConnection called %d times, want 2 (one per station attempt)", got)
	}
}

// TestListAccessPointsOverHTTP exercises GET /aps end-to-end: the handler calls
// through the Flow to the provisioner's Scan, which reads the fake's scan list
// and classifies each network's security from its raw D-Bus flags.
func TestListAccessPointsOverHTTP(t *testing.T) {
	fake.Reset()
	dev := fake.AddWiFiDevice("wlan0")
	// Flags mirror network.go: apflagsPrivacy=0x01, PSK=0x0100, SAE=0x0400.
	fake.SetAccessPoints(dev, []nmfake.AP{
		{SSID: "open-net", Strength: 30},
		{SSID: "wpa2-net", Strength: 70, Flags: 0x01, RsnFlags: 0x0100},
		{SSID: "sae-net", Strength: 55, Flags: 0x01, RsnFlags: 0x0400},
	})

	srv, _ := newFlowServer(t, "wlan0")

	resp, err := http.Get(srv.URL + "/aps")
	if err != nil {
		t.Fatalf("GET /aps: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /aps = %d, want 200", resp.StatusCode)
	}

	var body struct {
		Networks []provision.Network `json:"networks"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decoding /aps response: %v", err)
	}

	got := make(map[string]provision.NetworkSecurity, len(body.Networks))
	for _, n := range body.Networks {
		got[n.SSID] = n.Security
	}
	want := map[string]provision.NetworkSecurity{
		"open-net": provision.NetworkSecurityNone,
		"wpa2-net": provision.NetworkSecurityWpaPsk,
		"sae-net":  provision.NetworkSecuritySae,
	}
	for ssid, wantSec := range want {
		if got[ssid] != wantSec {
			t.Errorf("%s security = %v, want %v", ssid, got[ssid], wantSec)
		}
	}
}

// TestCredentialSubmissionStatusCodes exercises the POST /credentials status
// and error-code mapping through the real Flow.Submit, which — unlike the
// injected-error table in TestCredentialStateStatusCode — pins that the live
// state machine actually produces those errors at the moments claimed.
//
// The Flow's state is what decides the outcome: Submit checks it before
// validating, so the test runs in two phases, before and after Begin has
// parked the Flow in StateWaitingForCredentials.
//
// ErrSubmissionPending is deliberately not covered here. It isn't reachable
// deterministically against a live Flow — while waiting, the state machine is
// blocked receiving on the single-slot credentials channel, so a submission is
// consumed the instant it lands and the slot frees again.
func TestCredentialSubmissionStatusCodes(t *testing.T) {
	fake.Reset()
	fake.AddWiFiDevice("wlan0")
	fake.SetActivateOutcome(nmfake.ConnectScript()...)
	srv, flow := newFlowServer(t, "wlan0")

	// Phase 1: Begin has not run, so the Flow is StateIdle. The state check
	// short-circuits ahead of validation, so even well-formed credentials are
	// refused — and refused as a conflict (retryable once the AP is up), not as
	// a client error.
	errRes := postCredentials(t, srv.URL, "home-net", "good-password", http.StatusConflict)
	if errRes.Code != "ErrNotAwaitingCredentials" {
		t.Errorf("idle flow: code = %q, want ErrNotAwaitingCredentials", errRes.Code)
	}
	// Malformed JSON never reaches the Flow, so it is a 400 regardless of state.
	resp, err := http.Post(srv.URL+"/credentials", "application/json", strings.NewReader("{not json"))
	if err != nil {
		t.Fatalf("POST /credentials (malformed): %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("POST /credentials (malformed) = %d, want 400", resp.StatusCode)
	}

	// Phase 2: drive the Flow to StateWaitingForCredentials, where validation is
	// finally reached.
	events, closeStream := openEventStream(t, srv.URL)
	defer closeStream()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	beginErr := make(chan error, 1)
	go func() { beginErr <- flow.Begin(ctx) }()

	waitForState(t, events, iotflow.StateWaitingForCredentials, 5*time.Second)

	// Both validation sentinels surface as one client-facing code. Each leaves
	// the Flow waiting, so the next case can run against the same state.
	errRes = postCredentials(t, srv.URL, "home-net", "short", http.StatusBadRequest) // PSK < 8 chars
	if errRes.Code != "ErrInvalidCredentials" {
		t.Errorf("short psk: code = %q, want ErrInvalidCredentials", errRes.Code)
	}
	errRes = postCredentials(t, srv.URL, "", "good-password", http.StatusBadRequest) // empty SSID
	if errRes.Code != "ErrInvalidCredentials" {
		t.Errorf("empty ssid: code = %q, want ErrInvalidCredentials", errRes.Code)
	}

	// A well-formed submission is accepted and consumed — asserted by the Flow
	// running to completion, which it can only do on credentials it received.
	postCredentials(t, srv.URL, "home-net", "good-password", http.StatusAccepted)
	waitForState(t, events, iotflow.StateProvisioned, 5*time.Second)

	select {
	case err := <-beginErr:
		if err != nil {
			t.Fatalf("Begin returned %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Begin did not return after provisioning completed")
	}
}
