package iotflow

import (
	"fmt"

	"github.com/godbus/dbus/v5"
)

type deviceType uint32

const (
	deviceTypeUnknown      deviceType = 0
	deviceTypeEthernet     deviceType = 1
	deviceTypeWifi         deviceType = 2
	deviceTypeUnused1      deviceType = 3
	deviceTypeUnused2      deviceType = 4
	deviceTypeBt           deviceType = 5
	deviceTypeOlpcMesh     deviceType = 6
	deviceTypeWimax        deviceType = 7
	deviceTypeModem        deviceType = 8
	deviceTypeInfiniband   deviceType = 9
	deviceTypeBond         deviceType = 10
	deviceTypeVlan         deviceType = 11
	deviceTypeAdsl         deviceType = 12
	deviceTypeBridge       deviceType = 13
	deviceTypeGeneric      deviceType = 14
	deviceTypeTeam         deviceType = 15
	deviceTypeTun          deviceType = 16
	deviceTypeIpTunnel     deviceType = 17
	deviceTypeMacvlan      deviceType = 18
	deviceTypeVxlan        deviceType = 19
	deviceTypeVeth         deviceType = 20
	deviceTypeMacsec       deviceType = 21
	deviceTypeDummy        deviceType = 22
	deviceTypePpp          deviceType = 23
	deviceTypeOvsInterface deviceType = 24
	deviceTypeOvsPort      deviceType = 25
	deviceTypeOvsBridge    deviceType = 26
	deviceTypeWpan         deviceType = 27
	deviceType6lowpan      deviceType = 28
	deviceTypeWireguard    deviceType = 29
	deviceTypeWifiP2p      deviceType = 30
	deviceTypeVrf          deviceType = 31
)

func (d deviceType) String() string {
	switch d {
	case deviceTypeUnknown:
		return "unknown"
	case deviceTypeEthernet:
		return "ethernet"
	case deviceTypeWifi:
		return "wifi"
	case deviceTypeBt:
		return "bluetooth"
	case deviceTypeOlpcMesh:
		return "olpc-mesh"
	case deviceTypeWimax:
		return "wimax"
	case deviceTypeModem:
		return "modem"
	case deviceTypeInfiniband:
		return "infiniband"
	case deviceTypeBond:
		return "bond"
	case deviceTypeVlan:
		return "vlan"
	case deviceTypeAdsl:
		return "adsl"
	case deviceTypeBridge:
		return "bridge"
	case deviceTypeGeneric:
		return "generic"
	case deviceTypeTeam:
		return "team"
	case deviceTypeTun:
		return "tun"
	case deviceTypeIpTunnel:
		return "ip-tunnel"
	case deviceTypeMacvlan:
		return "macvlan"
	case deviceTypeVxlan:
		return "vxlan"
	case deviceTypeVeth:
		return "veth"
	case deviceTypeMacsec:
		return "macsec"
	case deviceTypeDummy:
		return "dummy"
	case deviceTypePpp:
		return "ppp"
	case deviceTypeOvsInterface:
		return "ovs-interface"
	case deviceTypeOvsPort:
		return "ovs-port"
	case deviceTypeOvsBridge:
		return "ovs-bridge"
	case deviceTypeWpan:
		return "wpan"
	case deviceType6lowpan:
		return "6lowpan"
	case deviceTypeWireguard:
		return "wireguard"
	case deviceTypeWifiP2p:
		return "wifi-p2p"
	case deviceTypeVrf:
		return "vrf"
	default:
		return "unknown"
	}
}

func getDeviceType(conn *dbus.Conn, devicePath dbus.ObjectPath) (deviceType, error) {
	variant, err := conn.Object(nmBusName, devicePath).GetProperty(
		"org.freedesktop.NetworkManager.Device.DeviceType",
	)
	if err != nil {
		return 0, err
	}

	t, ok := variant.Value().(uint32)
	if !ok {
		return 0, fmt.Errorf("unexpected device type value: %T", variant.Value())
	}
	return deviceType(t), nil
}
