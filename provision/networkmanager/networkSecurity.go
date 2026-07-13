package networkmanager

import "github.com/dawsonalex/iotflow/provision"

const (
	apflagsPrivacy = 0x01

	secKeyMgmtPSK       = 0x0100
	secKeyMgmt8021X     = 0x0200
	secKeyMgmtSAE       = 0x0400
	secKeyMgmtEAPSuiteB = 0x2000
)

func newNetworkSecurity(flagBits, wpaBits, rsnBits uint32) provision.NetworkSecurity {
	if (flagBits & apflagsPrivacy) == 0 {
		return provision.NetworkSecurityNone
	}

	combinedKeyFlags := wpaBits | rsnBits
	if combinedKeyFlags&secKeyMgmtSAE != 0 {
		return provision.NetworkSecuritySae
	}

	if combinedKeyFlags&secKeyMgmtPSK != 0 {
		return provision.NetworkSecurityWpaPsk
	}

	if combinedKeyFlags&secKeyMgmt8021X != 0 {
		return provision.NetworkSecurityWpaEap
	}

	if rsnBits&secKeyMgmtEAPSuiteB != 0 {
		return provision.NetworkSecurityWpaEapSuiteB
	}

	return provision.NetworkSecurityNone
}
