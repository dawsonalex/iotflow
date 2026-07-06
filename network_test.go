package iotflow

import (
	"testing"
)

func TestNetworkSecurityConstructor(t *testing.T) {
	tests := []struct {
		Name     string
		flagBits uint32
		wpaBits  uint32
		rsnBits  uint32
		expected NetworkSecurity
	}{
		{
			"open",
			0,
			0,
			0,
			NetworkSecurityNone,
		},
		{
			// WEP only sets the privacy bit; it advertises no WPA/RSN flags.
			"wep",
			apflagsPrivacy,
			0,
			0,
			NetworkSecurityNone,
		},
		{
			// Enterprise (wpa-eap) is 802.1X carried in the RSN element.
			"enterprise-wpa2",
			apflagsPrivacy,
			0,
			secKeyMgmt8021X,
			NetworkSecurityWpaEap,
		},
		{
			// WPA/WPA2 mixed enterprise: 802.1X advertised in both IEs.
			"enterprise-wpa-wpa2",
			apflagsPrivacy,
			secKeyMgmt8021X,
			secKeyMgmt8021X,
			NetworkSecurityWpaEap,
		},
		{
			// Suite-B-192 is RSN-only.
			"enterprise-suite-b-192",
			apflagsPrivacy,
			0,
			secKeyMgmtEAPSuiteB,
			NetworkSecurityWpaEapSuiteB,
		},
		{
			// Legacy WPA1-only PSK: PSK in the WPA element, no RSN.
			"wpa",
			apflagsPrivacy,
			secKeyMgmtPSK,
			0,
			NetworkSecurityWpaPsk,
		},
		{
			// WPA2 PSK: PSK in the RSN element.
			"wpa2",
			apflagsPrivacy,
			0,
			secKeyMgmtPSK,
			NetworkSecurityWpaPsk,
		},
		{
			// WPA/WPA2 mixed PSK: PSK advertised in both IEs.
			"wpa-wpa2",
			apflagsPrivacy,
			secKeyMgmtPSK,
			secKeyMgmtPSK,
			NetworkSecurityWpaPsk,
		},
		{
			// WPA3 personal: SAE is RSN-only.
			"wpa3",
			apflagsPrivacy,
			0,
			secKeyMgmtSAE,
			NetworkSecuritySae,
		},
		// Precedence cases: when an AP advertises more than one key-management
		// type, the soft-AP flow (open / PSK / SAE only) must report a
		// compatible option over an incompatible enterprise one, preferring the
		// strongest compatible option (SAE over PSK).
		{
			// WPA3 transition mode: RSN advertises both SAE and PSK. Both are
			// join-able; prefer the stronger SAE.
			"sae-preferred-over-psk",
			apflagsPrivacy,
			0,
			secKeyMgmtSAE | secKeyMgmtPSK,
			NetworkSecuritySae,
		},
		{
			// WPA2-Enterprise and WPA2-Personal together. Enterprise is not
			// usable by the soft-AP flow, so report the compatible PSK.
			"psk-preferred-over-enterprise",
			apflagsPrivacy,
			0,
			secKeyMgmt8021X | secKeyMgmtPSK,
			NetworkSecurityWpaPsk,
		},
		{
			// SAE alongside enterprise: SAE is compatible, 802.1X is not.
			"sae-preferred-over-enterprise",
			apflagsPrivacy,
			0,
			secKeyMgmtSAE | secKeyMgmt8021X,
			NetworkSecuritySae,
		},
		{
			// Everything at once: SAE outranks PSK, enterprise and Suite-B.
			"sae-outranks-all",
			apflagsPrivacy,
			0,
			secKeyMgmtSAE | secKeyMgmtPSK | secKeyMgmt8021X | secKeyMgmtEAPSuiteB,
			NetworkSecuritySae,
		},
		{
			// Flags spread across both IEs (enterprise in WPA1, PSK in RSN)
			// must still combine to the compatible PSK.
			"psk-in-rsn-over-enterprise-in-wpa",
			apflagsPrivacy,
			secKeyMgmt8021X,
			secKeyMgmtPSK,
			NetworkSecurityWpaPsk,
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			got := newNetworkSecurity(tt.flagBits, tt.wpaBits, tt.rsnBits)
			if tt.expected != got {
				t.Errorf("got %s, want %s", got, tt.expected)
			}
		})
	}
}
