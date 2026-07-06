package iotflow

type NetworkSecurity int

const (
	NetworkSecurityNone NetworkSecurity = iota
	NetworkSecurityOwe
	NetworkSecurityWpaPsk
	NetworkSecurityWpaEap
	NetworkSecuritySae
	NetworkSecurityWpaEapSuiteB
)

func (s NetworkSecurity) String() string {
	switch s {
	case NetworkSecurityNone:
		return "none"
	case NetworkSecurityOwe:
		return "owe"
	case NetworkSecurityWpaPsk:
		return "wpa-psk"
	case NetworkSecurityWpaEap:
		return "wpa-eap"
	case NetworkSecuritySae:
		return "sae"
	case NetworkSecurityWpaEapSuiteB:
		return "wpa-eap-suite-b-192"
	default:
		return "unknown"
	}
}

const (
	apflagsPrivacy = 0x01

	secKeyMgmtPSK       = 0x0100
	secKeyMgmt8021X     = 0x0200
	secKeyMgmtSAE       = 0x0400
	secKeyMgmtEAPSuiteB = 0x2000
)

func newNetworkSecurity(flagBits, wpaBits, rsnBits uint32) NetworkSecurity {
	if (flagBits & apflagsPrivacy) == 0 {
		return NetworkSecurityNone
	}

	combinedKeyFlags := wpaBits | rsnBits
	if combinedKeyFlags&secKeyMgmtSAE != 0 {
		return NetworkSecuritySae
	}

	if combinedKeyFlags&secKeyMgmtPSK != 0 {
		return NetworkSecurityWpaPsk
	}

	if combinedKeyFlags&secKeyMgmt8021X != 0 {
		return NetworkSecurityWpaEap
	}

	if rsnBits&secKeyMgmtEAPSuiteB != 0 {
		return NetworkSecurityWpaEapSuiteB
	}

	return NetworkSecurityNone
}

// Network represents a WiFi network that a provisioner can connect to
type Network struct {
	SSID     string
	Security NetworkSecurity
	Signal   uint8
}
