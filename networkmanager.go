package iotflow

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/godbus/dbus/v5"
	"github.com/google/uuid"
)

const (
	nmBusName    = "org.freedesktop.NetworkManager"
	nmObjectPath = "/org/freedesktop/NetworkManager"
)

var _ Provisioner = &NetworkManagerProvisioner{}

// NetworkManagerProvisioner is a provisioner that uses NetworkManager as a backend.
type NetworkManagerProvisioner struct {
	conn       *dbus.Conn
	ifacePath  dbus.ObjectPath
	activeConn dbus.ObjectPath
}

func NewNetworkManagerProvisioner(iface string) (*NetworkManagerProvisioner, error) {
	conn, err := dbus.SystemBus()
	if err != nil {
		return nil, fmt.Errorf("connecting to system bus: %w", err)
	}
	defer func() {
		_ = conn.Close()
	}()

	// TODO: This needs moving to a new 'begin'/'validate' style hook that provisioners run.
	var path dbus.ObjectPath
	if iface == "" {
		path, err = firstWiFiDevice(conn)
		if err != nil {
			return nil, fmt.Errorf("auto-discovering WiFi device: %w", err)
		}
	} else {
		path, err = deviceByIface(conn, iface)
		if err != nil {
			return nil, fmt.Errorf("looking up interface %q: %w", iface, err)
		}

		t, err := getDeviceType(conn, path)
		if err != nil {
			return nil, fmt.Errorf("checking device type for %q: %w", iface, err)
		}
		if t != deviceTypeWifi {
			return nil, fmt.Errorf("%q is not a WiFi device (got %s)", iface, t)
		}
	}

	return &NetworkManagerProvisioner{conn: conn, ifacePath: path}, nil
}

func (b *NetworkManagerProvisioner) IsConnected(_ context.Context) (bool, error) {
	state, err := getDeviceState(b.conn, b.ifacePath)
	if err != nil {
		return false, err
	}
	return state == nmDeviceStateActivated, nil
}

func (b *NetworkManagerProvisioner) EnableAPMode(ctx context.Context, ssid, psk string) (<-chan ProvisionUpdate, error) {
	if err := validateCredentials(ssid, psk); err != nil {
		return nil, err
	}

	settings := connectionSettings{
		"connection": {
			"id":          dbus.MakeVariant(ssid + "-ap"),
			"type":        dbus.MakeVariant("802-11-wireless"),
			"uuid":        dbus.MakeVariant(uuid.New().String()),
			"autoconnect": dbus.MakeVariant(false),
		},
		"802-11-wireless": {
			"ssid": dbus.MakeVariant([]byte(ssid)),
			"mode": dbus.MakeVariant("ap"),
		},
		"802-11-wireless-security": {
			"key-mgmt": dbus.MakeVariant("wpa-psk"),
			"psk":      dbus.MakeVariant(psk),
		},
		"ipv4": {
			"method": dbus.MakeVariant("shared"),
		},
	}

	activeConn, err := b.addAndActivateConnection(settings)
	if err != nil {
		return nil, err
	}
	b.activeConn = activeConn

	return b.pollProvisionUpdates(ctx, 100*time.Millisecond), nil
}

func (b *NetworkManagerProvisioner) DisableAPMode() error {
	if b.activeConn == "" {
		return errors.New("no active AP connection")
	}

	err := b.conn.Object(nmBusName, nmObjectPath).Call(
		"org.freedesktop.NetworkManager.DeactivateConnection", 0, b.activeConn,
	).Err
	if err != nil {
		return err
	}

	b.activeConn = ""
	return nil
}

func (b *NetworkManagerProvisioner) ConnectToNetwork(ctx context.Context, ssid, psk string) (<-chan ProvisionUpdate, error) {
	if err := validateCredentials(ssid, psk); err != nil {
		return nil, err
	}

	settings := connectionSettings{
		"connection": {
			"id":          dbus.MakeVariant(ssid + "-station"),
			"type":        dbus.MakeVariant("802-11-wireless"),
			"uuid":        dbus.MakeVariant(uuid.New().String()),
			"autoconnect": dbus.MakeVariant(true),
		},
		"802-11-wireless": {
			"ssid": dbus.MakeVariant([]byte(ssid)),
			"mode": dbus.MakeVariant("infrastructure"),
		},
		"802-11-wireless-security": {
			"key-mgmt": dbus.MakeVariant("wpa-psk"),
			"psk":      dbus.MakeVariant(psk),
		},
		"ipv4": {
			"method": dbus.MakeVariant("auto"),
		},
	}

	if _, err := b.addAndActivateConnection(settings); err != nil {
		return nil, err
	}

	return b.pollProvisionUpdates(ctx, 100*time.Millisecond), nil
}

func (b *NetworkManagerProvisioner) Close() error {
	return b.conn.Close()
}

func (b *NetworkManagerProvisioner) pollProvisionUpdates(ctx context.Context, tick time.Duration) <-chan ProvisionUpdate {
	ch := make(chan ProvisionUpdate)
	go func() {
		t := time.NewTicker(tick)
		defer t.Stop()
		defer close(ch)

		for {
			select {
			case <-t.C:
				state, err := getDeviceState(b.conn, b.ifacePath)
				if err != nil {
					ch <- ProvisionUpdate{State: ProvisionStateFailed, Err: err}
					return
				}

				update := toProvisionUpdate(state)
				ch <- update

				if update.State == ProvisionStateConnected || update.State == ProvisionStateFailed {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch
}

func toProvisionUpdate(s deviceState) ProvisionUpdate {
	switch s {
	case nmDeviceStateActivated:
		return ProvisionUpdate{State: ProvisionStateConnected}
	case nmDeviceStateFailed:
		return ProvisionUpdate{State: ProvisionStateFailed, Err: errors.New("connection failed")}
	default:
		return ProvisionUpdate{State: ProvisionStateConnecting}
	}
}
