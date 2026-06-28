package iotflow

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

type connectionSettings map[string]map[string]dbus.Variant

func (b *NetworkManagerProvisioner) addAndActivateConnection(settings connectionSettings) (dbus.ObjectPath, error) {
	var activeConn, connPath dbus.ObjectPath

	call := b.conn.Object(nmBusName, nmObjectPath).Call(
		"org.freedesktop.NetworkManager.AddAndActivateConnection",
		0,
		settings,
		b.ifacePath,
		dbus.ObjectPath("/"),
	)
	if call.Err != nil {
		return "", call.Err
	}

	if err := call.Store(&activeConn, &connPath); err != nil {
		return "", err
	}
	return activeConn, nil
}
