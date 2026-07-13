package nmfake

import (
	"fmt"
	"sync"
	"time"

	"github.com/godbus/dbus/v5"
	"github.com/godbus/dbus/v5/prop"
)

const (
	busName    = "org.freedesktop.NetworkManager"
	objectPath = "/org/freedesktop/NetworkManager"

	ifaceManager  = "org.freedesktop.NetworkManager"
	ifaceDevice   = "org.freedesktop.NetworkManager.Device"
	ifaceWireless = "org.freedesktop.NetworkManager.Device.Wireless"
	ifaceAP       = "org.freedesktop.NetworkManager.AccessPoint"

	// errScanNotAllowed is the D-Bus error NetworkManager returns for a
	// rate-limited scan; the provisioner treats it as "use the cached list".
	errScanNotAllowed = "org.freedesktop.NetworkManager.Device.NotAllowed"
)

// Device type and state constants mirror NetworkManager's so tests can script
// transitions without reaching into iotflow's unexported copies.
const (
	DeviceTypeWifi     uint32 = 2
	DeviceTypeEthernet uint32 = 1

	StateDisconnected uint32 = 30
	StatePrepare      uint32 = 40
	StateConfig       uint32 = 50
	StateActivated    uint32 = 100
	StateFailed       uint32 = 120
)

// AP describes one access point in a device's scan list. The raw flag fields
// map directly onto NetworkManager's AccessPoint.Flags/WpaFlags/RsnFlags, so a
// test drives the provisioner's NetworkSecurity classification by setting them.
type AP struct {
	SSID     string
	Strength uint8
	Flags    uint32
	WpaFlags uint32
	RsnFlags uint32
}

// StateStep is one step of a scripted Device.State transition applied (on a
// background goroutine) after AddAndActivateConnection is called.
type StateStep struct {
	State uint32
	Delay time.Duration
}

// ConnectScript walks Prepare -> Config -> Activated: the default successful
// activation.
func ConnectScript() []StateStep {
	return []StateStep{
		{StatePrepare, 5 * time.Millisecond},
		{StateConfig, 5 * time.Millisecond},
		{StateActivated, 5 * time.Millisecond},
	}
}

// FailScript walks Prepare -> Config -> Failed, simulating e.g. a bad PSK.
func FailScript() []StateStep {
	return []StateStep{
		{StatePrepare, 5 * time.Millisecond},
		{StateConfig, 5 * time.Millisecond},
		{StateFailed, 5 * time.Millisecond},
	}
}

// NM is a fake NetworkManager. It is safe for concurrent use: the production
// poller reads Device.State on one goroutine while the scripted activation
// mutates it on another.
type NM struct {
	conn *dbus.Conn

	mu             sync.Mutex
	devices        []*device
	nextDevID      int
	nextAPID       int
	nextActID      int
	scanCounter    int64
	activateScript []StateStep // outcome for station (infrastructure) activations
	apScript       []StateStep // outcome for AP (mode=ap) activations
	scanNotAllowed bool

	// captured calls, for assertions.
	lastAddSettings map[string]map[string]dbus.Variant
	addCalls        int
	deactivateCalls int
}

type device struct {
	path  dbus.ObjectPath
	iface string
	props *prop.Properties
}

// Start connects a fake NetworkManager to the bus at addr and claims the
// org.freedesktop.NetworkManager name. The returned cleanup closes the
// connection; callers must invoke it when finished.
func Start(addr string) (*NM, func(), error) {
	conn, err := dbus.Connect(addr)
	if err != nil {
		return nil, nil, fmt.Errorf("connecting fake NM to bus: %w", err)
	}

	nm := &NM{conn: conn, activateScript: ConnectScript(), apScript: ConnectScript()}

	if err := conn.Export(managerHandler{nm}, objectPath, ifaceManager); err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("exporting manager object: %w", err)
	}

	reply, err := conn.RequestName(busName, dbus.NameFlagDoNotQueue)
	if err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("requesting bus name: %w", err)
	}
	if reply != dbus.RequestNameReplyPrimaryOwner {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("could not own %s (reply %d) — is a real NetworkManager on this bus?", busName, reply)
	}

	return nm, func() { _ = conn.Close() }, nil
}

// Reset clears all devices, access points and captured calls and restores the
// default (successful) activation script. Call it at the start of each test.
//
// Object paths from previous tests remain exported on the bus but are no longer
// returned by GetDevices, so device discovery will not find them; each new
// device/AP is exported at a fresh, monotonically increasing path.
func (nm *NM) Reset() {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	nm.devices = nil
	nm.activateScript = ConnectScript()
	nm.apScript = ConnectScript()
	nm.scanNotAllowed = false
	nm.lastAddSettings = nil
	nm.addCalls = 0
	nm.deactivateCalls = 0
}

// AddWiFiDevice registers a WiFi device in the Disconnected state and returns
// its object path. Most tests add exactly one.
func (nm *NM) AddWiFiDevice(iface string) dbus.ObjectPath {
	return nm.addDevice(iface, DeviceTypeWifi, StateDisconnected)
}

// AddEthernetDevice registers a non-WiFi device, to exercise device-type checks.
func (nm *NM) AddEthernetDevice(iface string) dbus.ObjectPath {
	return nm.addDevice(iface, DeviceTypeEthernet, StateDisconnected)
}

func (nm *NM) addDevice(iface string, devType, state uint32) dbus.ObjectPath {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	nm.nextDevID++
	path := dbus.ObjectPath(fmt.Sprintf("%s/Devices/%d", objectPath, nm.nextDevID))

	props, err := prop.Export(nm.conn, path, prop.Map{
		ifaceDevice: {
			"DeviceType": {Value: devType, Emit: prop.EmitConst},
			"State":      {Value: state, Emit: prop.EmitFalse},
		},
		ifaceWireless: {
			"AccessPoints": {Value: []dbus.ObjectPath{}, Emit: prop.EmitFalse},
			"LastScan":     {Value: int64(1), Emit: prop.EmitFalse},
		},
	})
	if err != nil {
		panic(fmt.Errorf("nmfake: exporting device props: %w", err))
	}

	if err := nm.conn.Export(wirelessHandler{nm: nm, path: path}, path, ifaceWireless); err != nil {
		panic(fmt.Errorf("nmfake: exporting wireless iface: %w", err))
	}

	nm.devices = append(nm.devices, &device{path: path, iface: iface, props: props})
	return path
}

// SetDeviceState overrides a device's current State, e.g. to simulate an
// already-connected device before a test runs.
func (nm *NM) SetDeviceState(path dbus.ObjectPath, state uint32) {
	d := nm.device(path)
	if d == nil {
		panic(fmt.Errorf("nmfake: SetDeviceState: unknown device %s", path))
	}
	d.props.SetMust(ifaceDevice, "State", state)
}

// SetAccessPoints replaces the scan list reported for a device.
func (nm *NM) SetAccessPoints(path dbus.ObjectPath, aps []AP) {
	d := nm.device(path)
	if d == nil {
		panic(fmt.Errorf("nmfake: SetAccessPoints: unknown device %s", path))
	}

	nm.mu.Lock()
	apPaths := make([]dbus.ObjectPath, 0, len(aps))
	for _, ap := range aps {
		nm.nextAPID++
		apPath := dbus.ObjectPath(fmt.Sprintf("%s/AccessPoint/%d", objectPath, nm.nextAPID))
		if _, err := prop.Export(nm.conn, apPath, prop.Map{
			ifaceAP: {
				"Ssid":     {Value: []byte(ap.SSID), Emit: prop.EmitConst},
				"Strength": {Value: ap.Strength, Emit: prop.EmitConst},
				"Flags":    {Value: ap.Flags, Emit: prop.EmitConst},
				"WpaFlags": {Value: ap.WpaFlags, Emit: prop.EmitConst},
				"RsnFlags": {Value: ap.RsnFlags, Emit: prop.EmitConst},
			},
		}); err != nil {
			nm.mu.Unlock()
			panic(fmt.Errorf("nmfake: exporting AP props: %w", err))
		}
		apPaths = append(apPaths, apPath)
	}
	nm.mu.Unlock()

	d.props.SetMust(ifaceWireless, "AccessPoints", apPaths)
}

// SetActivateOutcome scripts the Device.State transitions applied after a
// station (mode=infrastructure) AddAndActivateConnection — i.e. ConnectToNetwork.
// Pass ConnectScript()..., FailScript()... or a custom sequence. AP-mode
// activations are unaffected; script those with SetAPActivateOutcome.
func (nm *NM) SetActivateOutcome(steps ...StateStep) {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	nm.activateScript = steps
}

// SetAPActivateOutcome scripts the Device.State transitions applied after an
// AP-mode (mode=ap) AddAndActivateConnection — i.e. EnableAPMode. It defaults to
// ConnectScript() so bringing the AP up succeeds; override it to simulate an AP
// that fails to start. Keeping AP and station outcomes separate lets a test fail
// a station connect without also failing the AP re-entry on the retry path.
func (nm *NM) SetAPActivateOutcome(steps ...StateStep) {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	nm.apScript = steps
}

// SetScanNotAllowed makes RequestScan return NetworkManager's rate-limit error,
// exercising the provisioner's fall-back-to-cached-list path.
func (nm *NM) SetScanNotAllowed(v bool) {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	nm.scanNotAllowed = v
}

// LastAddSettings returns the connection settings from the most recent
// AddAndActivateConnection call, or nil if none.
func (nm *NM) LastAddSettings() map[string]map[string]dbus.Variant {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	return nm.lastAddSettings
}

// AddActivateCalls returns how many times AddAndActivateConnection was called.
func (nm *NM) AddActivateCalls() int {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	return nm.addCalls
}

// DeactivateCalls returns how many times DeactivateConnection was called.
func (nm *NM) DeactivateCalls() int {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	return nm.deactivateCalls
}

func (nm *NM) device(path dbus.ObjectPath) *device {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	return nm.deviceLocked(path)
}

func (nm *NM) deviceLocked(path dbus.ObjectPath) *device {
	for _, d := range nm.devices {
		if d.path == path {
			return d
		}
	}
	return nil
}

// managerHandler exports the org.freedesktop.NetworkManager methods the
// provisioner calls on the manager object.
type managerHandler struct{ nm *NM }

func (h managerHandler) GetDevices() ([]dbus.ObjectPath, *dbus.Error) {
	h.nm.mu.Lock()
	defer h.nm.mu.Unlock()
	paths := make([]dbus.ObjectPath, 0, len(h.nm.devices))
	for _, d := range h.nm.devices {
		paths = append(paths, d.path)
	}
	return paths, nil
}

func (h managerHandler) GetDeviceByIpIface(iface string) (dbus.ObjectPath, *dbus.Error) {
	h.nm.mu.Lock()
	defer h.nm.mu.Unlock()
	for _, d := range h.nm.devices {
		if d.iface == iface {
			return d.path, nil
		}
	}
	return "/", dbus.NewError("org.freedesktop.NetworkManager.UnknownDevice",
		[]interface{}{fmt.Sprintf("no device named %q", iface)})
}

func (h managerHandler) AddAndActivateConnection(
	settings map[string]map[string]dbus.Variant,
	devicePath dbus.ObjectPath,
	_ dbus.ObjectPath,
) (dbus.ObjectPath, dbus.ObjectPath, *dbus.Error) {
	h.nm.mu.Lock()
	h.nm.addCalls++
	h.nm.lastAddSettings = settings
	h.nm.nextActID++
	actPath := dbus.ObjectPath(fmt.Sprintf("%s/ActiveConnection/%d", objectPath, h.nm.nextActID))
	connPath := dbus.ObjectPath(fmt.Sprintf("%s/Settings/%d", objectPath, h.nm.nextActID))
	// AP and station activations get independent outcomes so a scripted station
	// failure doesn't also fail the AP re-entry the provisioner does on retry.
	src := h.nm.activateScript
	if wirelessMode(settings) == "ap" {
		src = h.nm.apScript
	}
	script := append([]StateStep(nil), src...)
	d := h.nm.deviceLocked(devicePath)
	h.nm.mu.Unlock()

	if d != nil {
		go func() {
			for _, step := range script {
				time.Sleep(step.Delay)
				d.props.SetMust(ifaceDevice, "State", step.State)
			}
		}()
	}

	return actPath, connPath, nil
}

// wirelessMode extracts 802-11-wireless.mode ("ap" or "infrastructure") from a
// connection settings dict, or "" if absent. It selects which activation script
// applies.
func wirelessMode(settings map[string]map[string]dbus.Variant) string {
	sec, ok := settings["802-11-wireless"]
	if !ok {
		return ""
	}
	v, ok := sec["mode"]
	if !ok {
		return ""
	}
	mode, _ := v.Value().(string)
	return mode
}

func (h managerHandler) DeactivateConnection(_ dbus.ObjectPath) *dbus.Error {
	h.nm.mu.Lock()
	defer h.nm.mu.Unlock()
	h.nm.deactivateCalls++
	return nil
}

// wirelessHandler exports RequestScan for a single device path.
type wirelessHandler struct {
	nm   *NM
	path dbus.ObjectPath
}

func (h wirelessHandler) RequestScan(_ map[string]dbus.Variant) *dbus.Error {
	h.nm.mu.Lock()
	notAllowed := h.nm.scanNotAllowed
	h.nm.scanCounter++
	next := h.nm.scanCounter + 1
	d := h.nm.deviceLocked(h.path)
	h.nm.mu.Unlock()

	if notAllowed {
		return dbus.NewError(errScanNotAllowed, nil)
	}
	if d != nil {
		// Bump LastScan so the provisioner's awaitLastScanTimeUpdate returns.
		d.props.SetMust(ifaceWireless, "LastScan", next)
	}
	return nil
}
