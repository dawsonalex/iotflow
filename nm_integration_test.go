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
func newProvisioner(t *testing.T, iface string) *networkmanager.NetworkManagerProvisioner {
	t.Helper()
	p, err := networkmanager.NewNetworkManagerProvisioner(iface)
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
		if _, err := networkmanager.NewNetworkManagerProvisioner("eth0"); err == nil {
			t.Fatal("expected an error for a non-WiFi interface, got nil")
		}
	})

	t.Run("unknown interface is rejected", func(t *testing.T) {
		if _, err := networkmanager.NewNetworkManagerProvisioner("nope0"); err == nil {
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

	if err := p.DisableAPMode(); err != nil {
		t.Fatalf("DisableAPMode: %v", err)
	}
	if got := fake.DeactivateCalls(); got != 1 {
		t.Fatalf("DeactivateConnection called %d times, want 1", got)
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
