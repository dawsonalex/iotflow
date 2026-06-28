package iotflow

import (
	"errors"
)

type ProvisionState uint8

const (
	ProvisionStateConnecting ProvisionState = iota
	ProvisionStateConnected
	ProvisionStateFailed
)

func (s ProvisionState) String() string {
	switch s {
	case ProvisionStateConnecting:
		return "Connecting"
	case ProvisionStateConnected:
		return "Connected"
	case ProvisionStateFailed:
		return "Failed"
	default:
		return "Unknown"
	}
}

type ProvisionUpdate struct {
	State ProvisionState
	Err   error
}

var (
	ErrSSIDInvalid = errors.New("invalid ssid (must be between 1 and 32 characters)")
	ErrPSKInvalid  = errors.New("invalid psk (must be between 8 and 63 characters)")
)

func validateCredentials(ssid, psk string) error {
	if len(ssid) == 0 || len(ssid) > 32 {
		return ErrSSIDInvalid
	}
	if len(psk) < 8 || len(psk) > 63 {
		return ErrPSKInvalid
	}
	return nil
}
