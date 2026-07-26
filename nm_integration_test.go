//go:build integration

// Package iotflow_test integration suite: drives the real, unmodified
// NetworkManagerProvisioner against an in-process fake NetworkManager over a
// private D-Bus (see internal/nmfake). Run with:
//
//	go test -tags integration -race ./...
//
// Requires the `dbus-daemon` binary on PATH.
package iotflow_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/dawsonalex/iotflow/internal/nmfake"
	"github.com/dawsonalex/iotflow/provision"
	"github.com/dawsonalex/iotflow/provision/networkmanager"
	"github.com/godbus/dbus/v5"
	"github.com/stretchr/testify/assert"
)

// fake is the shared fake NetworkManager. One bus and one fake serve the whole
// package because godbus's dbus.SystemBus() caches its connection process-wide;
// each test calls fake.Reset() to start from a clean slate.
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

	// Redirect the provisioner's dbus.SystemBus() at our private bus.
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

// newProvisioner builds a provisioner against the shared fake and closes it at
// test end. Closing disconnects the cached system-bus connection; the next
// construction transparently reconnects (godbus re-reads the env address).
func newProvisioner(t *testing.T, iface string) *networkmanager.Provisioner {
	t.Helper()
	p, err := networkmanager.NewProvisioner(iface)
	if err != nil {
		t.Fatalf("NewNetworkManagerProvisioner(%q): %v", iface, err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

// drain reads a ProvisionUpdate stream to completion and returns the last update.
func drain(t *testing.T, ch <-chan provision.Update) provision.Update {
	t.Helper()
	var last provision.Update
	seen := false
	timeout := time.After(3 * time.Second)
	for {
		select {
		case u, ok := <-ch:
			if !ok {
				if !seen {
					t.Fatal("update channel closed without any updates")
				}
				return last
			}
			last, seen = u, true
		case <-timeout:
			t.Fatal("timed out waiting for provision updates")
		}
	}
}

// assertVariantString asserts settings[section][key] is a string equal to want.
func assertVariantString(t *testing.T, settings map[string]map[string]dbus.Variant, section, key, want string) {
	t.Helper()
	sec, ok := settings[section]
	if !ok {
		t.Fatalf("settings missing section %q", section)
	}
	v, ok := sec[key]
	if !ok {
		t.Fatalf("settings[%q] missing key %q", section, key)
	}
	got, ok := v.Value().(string)
	if !ok {
		t.Fatalf("settings[%q][%q] is %T, want string", section, key, v.Value())
	}
	if got != want {
		t.Fatalf("settings[%q][%q] = %q, want %q", section, key, got, want)
	}
}

func TestDeviceDiscovery(t *testing.T) {
	fake.Reset()
	// Ethernet first, WiFi second: auto-discovery must skip the non-WiFi device.
	fake.AddEthernetDevice("eth0")
	fake.AddWiFiDevice("wlan0")

	t.Run("auto-discovers the wifi device", func(t *testing.T) {
		newProvisioner(t, "") // construction alone proves a WiFi device was found
	})

	t.Run("explicit wifi interface", func(t *testing.T) {
		newProvisioner(t, "wlan0")
	})

	t.Run("explicit non-wifi interface is rejected", func(t *testing.T) {
		if _, err := networkmanager.NewProvisioner("eth0"); err == nil {
			t.Fatal("expected an error for a non-WiFi interface, got nil")
		}
	})

	t.Run("unknown interface is rejected", func(t *testing.T) {
		if _, err := networkmanager.NewProvisioner("nope0"); err == nil {
			t.Fatal("expected an error for an unknown interface, got nil")
		}
	})
}

func TestIsConnectedReflectsDeviceState(t *testing.T) {
	fake.Reset()
	dev := fake.AddWiFiDevice("wlan0")
	p := newProvisioner(t, "wlan0")
	ctx := context.Background()

	if ok, err := p.IsConnected(ctx); err != nil || ok {
		t.Fatalf("disconnected device: IsConnected = (%v, %v), want (false, nil)", ok, err)
	}

	fake.SetDeviceState(dev, nmfake.StateActivated)

	if ok, err := p.IsConnected(ctx); err != nil || !ok {
		t.Fatalf("activated device: IsConnected = (%v, %v), want (true, nil)", ok, err)
	}
}

func TestEnableAndDisableAPMode(t *testing.T) {
	fake.Reset()
	fake.AddWiFiDevice("wlan0")
	p := newProvisioner(t, "wlan0")

	ch, err := p.EnableAPMode(context.Background(), "my-iot-ap", "supersecret")
	if err != nil {
		t.Fatalf("EnableAPMode: %v", err)
	}
	if final := drain(t, ch); final.State != provision.StateConnected {
		t.Fatalf("final AP state = %v (err %v), want Connected", final.State, final.Err)
	}

	// The provisioner must have sent AP-shaped connection settings.
	settings := fake.LastAddSettings()
	if settings == nil {
		t.Fatal("AddAndActivateConnection was never called")
	}
	assertVariantString(t, settings, "connection", "id", "my-iot-ap-ap")
	assertVariantString(t, settings, "802-11-wireless", "mode", "ap")
	assertVariantString(t, settings, "ipv4", "method", "shared")

	if err := p.DisableAPMode(context.Background()); err != nil {
		t.Fatalf("DisableAPMode: %v", err)
	}
	if got := fake.DeactivateCalls(); got != 1 {
		t.Fatalf("DeactivateConnection called %d times, want 1", got)
	}
	// Deactivating leaves the profile in NetworkManager's configuration, where it
	// survives reboots. A clean teardown deletes it too.
	if got := fake.DeletedConns(); len(got) != 1 {
		t.Fatalf("deleted %d connection profiles, want 1 (leaked: %v)", len(got), got)
	}
}

// TestAPProfileNotLeakedOnRetry covers the path the Flow takes when DisableAPMode
// fails: it loops back to EnableAPMode. Every EnableAPMode call adds a profile
// with a fresh UUID, so unless the previous one is cleaned up the device
// accumulates a dead AP profile per attempt, and every profile but the last
// becomes unreachable — nothing retains its path.
func TestAPProfileNotLeakedOnRetry(t *testing.T) {
	fake.Reset()
	fake.AddWiFiDevice("wlan0")
	p := newProvisioner(t, "wlan0")
	ctx := context.Background()

	const attempts = 3
	for i := range attempts {
		ch, err := p.EnableAPMode(ctx, "my-iot-ap", "supersecret")
		if err != nil {
			t.Fatalf("attempt %d: EnableAPMode: %v", i, err)
		}
		if final := drain(t, ch); final.State != provision.StateConnected {
			t.Fatalf("attempt %d: final AP state = %v (err %v), want Connected", i, final.State, final.Err)
		}
	}

	// Each re-entry cleans up its predecessor, so only the newest profile is
	// still around at this point.
	if got := fake.DeletedConns(); len(got) != attempts-1 {
		t.Fatalf("after %d EnableAPMode calls, deleted %d profiles, want %d (deleted: %v)",
			attempts, len(got), attempts-1, got)
	}

	if err := p.DisableAPMode(ctx); err != nil {
		t.Fatalf("DisableAPMode: %v", err)
	}
	if got := fake.DeletedConns(); len(got) != attempts {
		t.Fatalf("after teardown, deleted %d profiles, want %d (leaked: %v)",
			len(got), attempts, got)
	}
}

func TestConnectToNetwork(t *testing.T) {
	tests := []struct {
		name   string
		script []nmfake.StateStep
		want   provision.State
	}{
		{"reaches connected on success", nmfake.ConnectScript(), provision.StateConnected},
		{"reaches failed on failure", nmfake.FailScript(), provision.StateFailed},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake.Reset()
			fake.AddWiFiDevice("wlan0")
			fake.SetActivateOutcome(tc.script...)
			p := newProvisioner(t, "wlan0")

			ch, err := p.ConnectToNetwork(context.Background(), "home-wifi", "password123")
			if err != nil {
				t.Fatalf("ConnectToNetwork: %v", err)
			}
			if final := drain(t, ch); final.State != tc.want {
				t.Fatalf("final state = %v (err %v), want %v", final.State, final.Err, tc.want)
			}

			settings := fake.LastAddSettings()
			if settings == nil {
				t.Fatal("AddAndActivateConnection was never called")
			}
			assertVariantString(t, settings, "connection", "id", "home-wifi-station")
			assertVariantString(t, settings, "802-11-wireless", "mode", "infrastructure")
			assertVariantString(t, settings, "ipv4", "method", "auto")
		})
	}

	t.Run("invalid psk is rejected before touching the bus", func(t *testing.T) {
		fake.Reset()
		fake.AddWiFiDevice("wlan0")
		p := newProvisioner(t, "wlan0")

		if _, err := p.ConnectToNetwork(context.Background(), "home-wifi", "short"); err == nil {
			t.Fatal("expected an error for a too-short PSK, got nil")
		}
		if got := fake.AddActivateCalls(); got != 0 {
			t.Fatalf("AddAndActivateConnection called %d times for invalid creds, want 0", got)
		}
	})
}

func TestScan(t *testing.T) {
	t.Run("classifies security from access-point flags", func(t *testing.T) {
		fake.Reset()
		dev := fake.AddWiFiDevice("wlan0")
		// Flag values mirror network.go: apflagsPrivacy=0x01, PSK=0x0100, SAE=0x0400.
		fake.SetAccessPoints(dev, []nmfake.AP{
			{SSID: "open-net", Strength: 40},
			{SSID: "wpa2-net", Strength: 80, Flags: 0x01, RsnFlags: 0x0100},
			{SSID: "sae-net", Strength: 60, Flags: 0x01, RsnFlags: 0x0400},
		})
		p := newProvisioner(t, "wlan0")

		nets, err := p.Scan(context.Background())
		if err != nil {
			t.Fatalf("Scan: %v", err)
		}

		security := make(map[string]provision.NetworkSecurity, len(nets))
		signal := make(map[string]uint8, len(nets))
		for _, n := range nets {
			security[n.SSID] = n.Security
			signal[n.SSID] = n.Signal
		}

		wantSecurity := map[string]provision.NetworkSecurity{
			"open-net": provision.NetworkSecurityNone,
			"wpa2-net": provision.NetworkSecurityWpaPsk,
			"sae-net":  provision.NetworkSecuritySae,
		}
		for ssid, want := range wantSecurity {
			if got := security[ssid]; got != want {
				t.Errorf("%s security = %v, want %v", ssid, got, want)
			}
		}
		if got := signal["wpa2-net"]; got != 80 {
			t.Errorf("wpa2-net signal = %d, want 80", got)
		}
	})

	t.Run("falls back to cached list when scan is rate-limited", func(t *testing.T) {
		fake.Reset()
		dev := fake.AddWiFiDevice("wlan0")
		fake.SetAccessPoints(dev, []nmfake.AP{{SSID: "cached-net", Strength: 55}})
		fake.SetScanNotAllowed(true)
		p := newProvisioner(t, "wlan0")

		nets, err := p.Scan(context.Background())
		if err != nil {
			t.Fatalf("Scan (rate-limited): %v", err)
		}
		if len(nets) != 1 || nets[0].SSID != "cached-net" {
			t.Fatalf("Scan returned %+v, want the single cached network", nets)
		}
	})
}

func TestPollProvisionUpdatesErrorClosesChannel(t *testing.T) {
	fake.Reset()
	dev := fake.AddWiFiDevice("wlan0")
	p := newProvisioner(t, "wlan0")

	ch, err := p.ConnectToNetwork(context.Background(), "home-wifi", "password123")
	assert.Nil(t, err)

	// This only works because the networkmanager.Provisioner is polling every 100ms.
	// We're expecting ForceDeviceError to run before the first poll, so it comes back
	// with an error.
	fake.ForceDeviceError(dev)

	updates := waitForClose(t, ch)

	// we should see exactly one item, just the failure, and it should have an error.
	assert.Equal(t, 1, len(updates))
	assert.NotNil(t, updates[0].Err)
	assert.Equal(t, updates[0].State, provision.StateFailed)
}

// waitForClose reads from ch until it is either closed, or waitTimeout is reached,
// in which case t is failed. When ch is closed, a slice of T is returned.
func waitForClose[T any](t *testing.T, ch <-chan T) []T {
	t.Helper()

	items := make([]T, 0)
	timeout := time.After(waitTimeout)
	for {
		select {
		case v, ok := <-ch:
			if !ok {
				return items
			}
			items = append(items, v)
		case <-timeout:
			t.Fatalf("timed out after %s waiting for channel to close", waitTimeout)
		}
	}
}

// TestEnableAPModeKeepsOldAPUntilReplacementIsUp pins the ordering inside
// EnableAPMode. The AP the user is associated with is their only route back
// into the device, and EnableAPMode failing is terminal for the Flow — so if
// the old AP were torn down before the new one activated, a failed activation
// would leave the device with no AP, no station connection and a finished Flow.
// That needs physical access to undo.
func TestEnableAPModeKeepsOldAPUntilReplacementIsUp(t *testing.T) {
	fake.Reset()
	fake.AddWiFiDevice("wlan0")
	p := newProvisioner(t, "wlan0")
	ctx := context.Background()

	ch, err := p.EnableAPMode(ctx, "my-iot-ap", "supersecret")
	if err != nil {
		t.Fatalf("EnableAPMode: %v", err)
	}
	if final := drain(t, ch); final.State != provision.StateConnected {
		t.Fatalf("final AP state = %v, want Connected", final.State)
	}

	live := fake.LiveConns()
	if len(live) != 1 {
		t.Fatalf("after one EnableAPMode, live profiles = %v, want exactly 1", live)
	}
	original := live[0]

	// Re-enter AP mode the way the retry path does. The original profile must
	// survive until the replacement exists — asserted by the fake, which errors
	// on a Delete for a path it never issued or already removed, so a delete
	// happening at the wrong time cannot pass silently.
	ch, err = p.EnableAPMode(ctx, "my-iot-ap", "supersecret")
	if err != nil {
		t.Fatalf("second EnableAPMode: %v", err)
	}
	if final := drain(t, ch); final.State != provision.StateConnected {
		t.Fatalf("second final AP state = %v, want Connected", final.State)
	}

	live = fake.LiveConns()
	if len(live) != 1 {
		t.Fatalf("after re-entry, live profiles = %v, want exactly 1", live)
	}
	if live[0] == original {
		t.Fatal("re-entry reused the original profile; expected a fresh one")
	}
	if deleted := fake.DeletedConns(); len(deleted) != 1 || deleted[0] != original {
		t.Fatalf("deleted = %v, want exactly the original profile %q", deleted, original)
	}
}

// TestStationProfileNotLeakedOnRetry covers the connect-failure counterpart.
// A profile from a previous ConnectToNetwork is by definition a failed attempt
// — a successful one ends the Flow — and it carries the PSK the user got wrong
// with autoconnect: true, so leaving it behind means NetworkManager keeps
// retrying a known-bad credential for that SSID across reboots.
func TestStationProfileNotLeakedOnRetry(t *testing.T) {
	fake.Reset()
	fake.AddWiFiDevice("wlan0")
	p := newProvisioner(t, "wlan0")
	ctx := context.Background()

	const attempts = 3
	for i := range attempts {
		ch, err := p.ConnectToNetwork(ctx, "home-wifi", "wrongpassword")
		if err != nil {
			t.Fatalf("attempt %d: ConnectToNetwork: %v", i, err)
		}
		drain(t, ch)
	}

	if live := fake.LiveConns(); len(live) != 1 {
		t.Fatalf("after %d connect attempts, live profiles = %v, want exactly 1", attempts, live)
	}
	if got := fake.DeletedConns(); len(got) != attempts-1 {
		t.Fatalf("deleted %d station profiles, want %d (leaked: %v)", len(got), attempts-1, got)
	}
}
