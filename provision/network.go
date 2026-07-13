package provision

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

// Network represents a WiFi network that a provisioner can connect to
type Network struct {
	SSID     string
	Security NetworkSecurity
	Signal   uint8
}
