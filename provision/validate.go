package provision

import (
	"errors"
)

var (
	ErrSSIDInvalid = errors.New("invalid ssid (must be between 1 and 32 characters)")
	ErrPSKInvalid  = errors.New("invalid psk (must be between 8 and 63 characters)")
)

func ValidateCredentials(ssid, psk string) error {
	if len(ssid) == 0 || len(ssid) > 32 {
		return ErrSSIDInvalid
	}
	if len(psk) < 8 || len(psk) > 63 {
		return ErrPSKInvalid
	}
	return nil
}
