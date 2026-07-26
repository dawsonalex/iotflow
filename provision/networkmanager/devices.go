package networkmanager

import (
	"errors"
	"fmt"

	"github.com/godbus/dbus/v5"
)

func getDevices(conn *dbus.Conn) ([]dbus.ObjectPath, error) {
	call := conn.Object(nmBusName, nmObjectPath).Call(
		"org.freedesktop.NetworkManager.GetDevices", 0,
	)
	if call.Err != nil {
		return nil, call.Err
	}

	var devices []dbus.ObjectPath
	if err := call.Store(&devices); err != nil {
		return nil, err
	}
	return devices, nil
}

func firstWiFiDevice(conn *dbus.Conn) (dbus.ObjectPath, error) {
	devices, err := getDevices(conn)
	if err != nil {
		return "", err
	}

	for _, d := range devices {
		t, err := getDeviceType(conn, d)
		if err != nil {
			continue
		}
		if t == deviceTypeWifi {
			return d, nil
		}
	}

	return "", errors.New("no WiFi device found")
}

func deviceByIface(conn *dbus.Conn, iface string) (dbus.ObjectPath, error) {
	call := conn.Object(nmBusName, nmObjectPath).Call(
		"org.freedesktop.NetworkManager.GetDeviceByIpIface", 0, iface,
	)
	if call.Err != nil {
		return "", call.Err
	}

	var path dbus.ObjectPath
	if err := call.Store(&path); err != nil {
		return "", err
	}
	return path, nil
}

func getDeviceState(conn *dbus.Conn, devicePath dbus.ObjectPath) (deviceState, error) {
	variant, err := conn.Object(nmBusName, devicePath).GetProperty(
		"org.freedesktop.NetworkManager.Device.State",
	)
	if err != nil {
		return 0, err
	}

	v, ok := variant.Value().(uint32)
	if !ok {
		return 0, fmt.Errorf("unexpected device state value: %T", variant.Value())
	}
	return deviceState(v), nil
}

// connectionSettings is the variant map that gets passed to Provisioner.addAndActivateConnection
// for information on the shape of the map, check man pages nm-settings-dbus(5).
// TODO: This could become a struct that contains specific values, which could be serialised to the variant map.
type connectionSettings map[string]map[string]dbus.Variant

// addAndActivateConnection adds a new connection profile and activates it,
// returning the active-connection path and the profile's settings path. Both
// matter: the first is what DeactivateConnection takes, the second is what
// Delete takes. A caller that keeps only the first can activate a profile it
// can never remove, and NetworkManager persists profiles across reboots.
func (b *Provisioner) addAndActivateConnection(settings connectionSettings) (activeConn, connPath dbus.ObjectPath, err error) {
	call := b.conn.Object(nmBusName, nmObjectPath).Call(
		"org.freedesktop.NetworkManager.AddAndActivateConnection",
		0,
		settings,
		b.ifacePath,
		dbus.ObjectPath("/"),
	)
	if call.Err != nil {
		return "", "", call.Err
	}

	if err := call.Store(&activeConn, &connPath); err != nil {
		return "", "", err
	}
	return activeConn, connPath, nil
}

// deleteConnection removes a connection profile from NetworkManager's
// configuration. Deleting an active profile also deactivates it. An empty path
// is a no-op: dbus.Object accepts one and the call would go somewhere
// unintended rather than failing cleanly.
func (b *Provisioner) deleteConnection(path dbus.ObjectPath) error {
	if path == "" {
		return nil
	}
	return b.conn.Object(nmBusName, path).Call(
		"org.freedesktop.NetworkManager.Settings.Connection.Delete", 0,
	).Err
}
