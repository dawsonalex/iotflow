package provision

import (
	"context"
)

// Provisioner provides an interface to the subsystem that handles network configuration
// (e.g. NetworkManager, or wpa_supplicant, etc).
// The creator of a Provisioner must remember to call Close() when finished provisioning to
// prevent resource leaks of the underlying implementation.
type Provisioner interface {
	IsConnected(context.Context) (bool, error)
	EnableAPMode(context.Context, string, string) (<-chan Update, error)
	DisableAPMode(ctx context.Context) error
	ConnectToNetwork(context.Context, string, string) (<-chan Update, error)
	Close() error
	Scan(context.Context) ([]Network, error)
}

type State uint8

const (
	StateConnecting State = iota
	StateConnected
	StateFailed
)

func (s State) String() string {
	switch s {
	case StateConnecting:
		return "Connecting"
	case StateConnected:
		return "Connected"
	case StateFailed:
		return "Failed"
	default:
		return "Unknown"
	}
}

type Update struct {
	State State
	Err   error
}
