package networkmanager

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dawsonalex/iotflow/provision"
	"github.com/godbus/dbus/v5"
	"github.com/google/uuid"
)

const (
	nmBusName    = "org.freedesktop.NetworkManager"
	nmObjectPath = "/org/freedesktop/NetworkManager"
)

var _ provision.Provisioner = &Provisioner{}

// Provisioner is a provisioner that uses NetworkManager as a backend.
type Provisioner struct {
	conn       *dbus.Conn
	ifacePath  dbus.ObjectPath
	activeConn dbus.ObjectPath
}

// NewProvisioner creates a new Provisioner that operates on NetworkManager over DBus.
// The returned provisioner must be closed by the caller when provisioning is complete by
// calling `Close()` on the returned Provisioner.
func NewProvisioner(iface string) (*Provisioner, error) {
	conn, err := dbus.SystemBus()
	if err != nil {
		return nil, fmt.Errorf("connecting to system bus: %w", err)
	}

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

	return &Provisioner{conn: conn, ifacePath: path}, nil
}

func (b *Provisioner) IsConnected(_ context.Context) (bool, error) {
	state, err := getDeviceState(b.conn, b.ifacePath)
	if err != nil {
		return false, err
	}
	return state == nmDeviceStateActivated, nil
}

func (b *Provisioner) EnableAPMode(ctx context.Context, ssid, psk string) (<-chan provision.Update, error) {
	if err := provision.ValidateCredentials(ssid, psk); err != nil {
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

func (b *Provisioner) DisableAPMode() error {
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

func (b *Provisioner) ConnectToNetwork(ctx context.Context, ssid, psk string) (<-chan provision.Update, error) {
	if err := provision.ValidateCredentials(ssid, psk); err != nil {
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

func (b *Provisioner) Close() error {
	return b.conn.Close()
}

func (b *Provisioner) pollProvisionUpdates(ctx context.Context, tick time.Duration) <-chan provision.Update {
	ch := make(chan provision.Update)
	go func() {
		t := time.NewTicker(tick)
		defer t.Stop()
		defer close(ch)

		for {
			select {
			case <-t.C:
				state, err := getDeviceState(b.conn, b.ifacePath)
				if err != nil {
					ch <- provision.Update{State: provision.StateFailed, Err: err}
					return
				}

				update := toProvisionUpdate(state)
				ch <- update

				if update.State == provision.StateConnected || update.State == provision.StateFailed {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch
}

func toProvisionUpdate(s deviceState) provision.Update {
	switch s {
	case nmDeviceStateActivated:
		return provision.Update{State: provision.StateConnected}
	case nmDeviceStateFailed:
		return provision.Update{State: provision.StateFailed, Err: errors.New("connection failed")}
	default:
		return provision.Update{State: provision.StateConnecting}
	}
}

const dbusErrNotAllowed = "org.freedesktop.NetworkManager.Device.NotAllowed"

func (b *Provisioner) Scan(ctx context.Context) ([]provision.Network, error) {
	t, err := b.lastScanTime(ctx)
	if err != nil {
		return nil, err
	}
	shouldAwaitScan, err := b.requestScan(ctx)
	if err != nil {
		return nil, err
	}

	if shouldAwaitScan {
		if err = b.awaitLastScanTimeUpdate(ctx, t); err != nil {
			return nil, err
		}
	}

	return b.getNetworkList(ctx)
}

const nmAccessPointIface = "org.freedesktop.NetworkManager.AccessPoint"

func (b *Provisioner) getNetworkList(ctx context.Context) ([]provision.Network, error) {
	accessPointsVariant, err := b.conn.Object(nmBusName, b.ifacePath).GetProperty("org.freedesktop.NetworkManager.Device.Wireless.AccessPoints")
	if err != nil {
		return nil, fmt.Errorf("getting access points: %w", err)
	}

	var accessPoints []dbus.ObjectPath
	err = accessPointsVariant.Store(&accessPoints)
	if err != nil {
		return nil, fmt.Errorf("storing access points: %w", err)
	}

	networks := make([]provision.Network, 0, len(accessPoints))
	for _, apPath := range accessPoints {
		ap := b.conn.Object(nmBusName, apPath)

		ssidVariant, err := ap.GetProperty(nmAccessPointIface + ".Ssid")
		if err != nil {
			continue
		}
		var ssidBytes []byte
		if err := ssidVariant.Store(&ssidBytes); err != nil {
			continue
		}

		strengthVariant, err := ap.GetProperty(nmAccessPointIface + ".Strength")
		if err != nil {
			continue
		}
		var strength uint8
		if err := strengthVariant.Store(&strength); err != nil {
			continue
		}

		var security provision.NetworkSecurity
		if security, err = b.accessPointSecurity(apPath); err != nil {
			continue
		}

		networks = append(networks, provision.Network{
			SSID:     string(ssidBytes),
			Signal:   strength,
			Security: security,
		})
	}

	return networks, nil
}

// calculates the security that an access point has (wpa, wpa2, etc)
func (b *Provisioner) accessPointSecurity(apPath dbus.ObjectPath) (provision.NetworkSecurity, error) {
	flagsVariant, err := b.conn.Object(nmBusName, apPath).GetProperty("org.freedesktop.NetworkManager.AccessPoint.Flags")
	if err != nil {
		return provision.NetworkSecurityNone, fmt.Errorf("getting security flags: %w", err)
	}

	var flags uint32
	if err = flagsVariant.Store(&flags); err != nil {
		return provision.NetworkSecurityNone, fmt.Errorf("storing security flags: %w", err)
	}

	wpaSecVariant, err := b.conn.Object(nmBusName, apPath).GetProperty("org.freedesktop.NetworkManager.AccessPoint.WpaFlags")
	if err != nil {
		return provision.NetworkSecurityNone, fmt.Errorf("getting WPA flags: %w", err)
	}

	var wpaFlags uint32
	if err = wpaSecVariant.Store(&wpaFlags); err != nil {
		return provision.NetworkSecurityNone, fmt.Errorf("storing WPA flags: %w", err)
	}

	rsnFlagsVariant, err := b.conn.Object(nmBusName, apPath).GetProperty("org.freedesktop.NetworkManager.AccessPoint.RsnFlags")
	if err != nil {
		return provision.NetworkSecurityNone, fmt.Errorf("getting RSN flags: %w", err)
	}

	var rsnFlags uint32
	if err = rsnFlagsVariant.Store(&rsnFlags); err != nil {
		return provision.NetworkSecurityNone, fmt.Errorf("storing RSN flags: %w", err)
	}

	return newNetworkSecurity(flags, wpaFlags, rsnFlags), nil
}

// requestScan requests a re-scan of the AP list, returning a bool that indicates whether the caller should
// await the list updating, alongside an error.
func (b *Provisioner) requestScan(ctx context.Context) (bool, error) {
	call := b.conn.Object(nmBusName, b.ifacePath).CallWithContext(
		ctx,
		"org.freedesktop.NetworkManager.Device.Wireless.RequestScan",
		0,
		map[string]dbus.Variant{},
	)
	if call.Err != nil {
		var errDBus dbus.Error
		// dbusErrNotAllowed often occurs due to rate limiting by NetworkManager when requesting scans.
		// We can fall through to using existing AP list instead.
		if errors.As(call.Err, &errDBus) && errDBus.Name == dbusErrNotAllowed {
			return false, nil
		}

		return false, fmt.Errorf("requesting scan: %w", call.Err)
	}

	return true, nil
}

func (b *Provisioner) lastScanTime(ctx context.Context) (int64, error) {
	var lastScanTime int64
	lastScanVariant, err := b.conn.Object(nmBusName, b.ifacePath).GetProperty("org.freedesktop.NetworkManager.Device.Wireless.LastScan")
	if err != nil {
		return 0, err
	}
	err = lastScanVariant.Store(&lastScanTime)
	if err != nil {
		return 0, err
	}

	return lastScanTime, nil
}

func (b *Provisioner) awaitLastScanTimeUpdate(ctx context.Context, startTime int64) error {
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()

	for range t.C {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			scanTime, err := b.lastScanTime(ctx)
			if err != nil {
				return fmt.Errorf("awaiting last scan update: %w", err)
			}

			if scanTime > startTime {
				return nil
			}
		}
	}

	return nil
}
