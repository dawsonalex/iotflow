package iotflow

import (
	"context"
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

type Backend interface {
	IsConnected(ctx context.Context) (bool, error)
	EnableAPMode(ctx context.Context, ssid, psk string) (<-chan ProvisionUpdate, error)
	DisableAPMode() error
	ConnectToNetwork(ctx context.Context, ssid, psk string) (<-chan ProvisionUpdate, error)
	Close() error
}

type provisionerConfig struct {
	iface string // empty triggers auto-discovery
}

// Option configures a Provisioner at construction time.
type Option func(*provisionerConfig)

// WithInterface pins the Provisioner to a specific WiFi interface (e.g. "wlan0").
// Without this option, the first available WiFi device is used.
func WithInterface(iface string) Option {
	return func(c *provisionerConfig) {
		c.iface = iface
	}
}

// Provisioner manages WiFi provisioning for an IoT device.
// Although you can create a Provisioner directly, it's sensible to
// use the NewProvisioner constructor, or NewNmProvisioner for NetworkManager backends.
type Provisioner struct {
	b Backend
}

// NewProvisioner returns a Provisioner backed by NetworkManager. With no options it
// auto-discovers the first available WiFi device. Use WithInterface to target a
// specific adapter.
func NewProvisioner(backend Backend) *Provisioner {
	return &Provisioner{b: backend}
}

func NewNmProvisioner(opts ...Option) (*Provisioner, error) {
	cfg := &provisionerConfig{}
	for _, o := range opts {
		o(cfg)
	}

	b, err := newNMBackend(cfg)
	if err != nil {
		return nil, err
	}

	return &Provisioner{b: b}, nil
}

func (p *Provisioner) IsConnected(ctx context.Context) (bool, error) {
	return p.b.IsConnected(ctx)
}

func (p *Provisioner) EnableAPMode(ctx context.Context, ssid, psk string) (<-chan ProvisionUpdate, error) {
	if err := validateCredentials(ssid, psk); err != nil {
		return nil, err
	}
	return p.b.EnableAPMode(ctx, ssid, psk)
}

func (p *Provisioner) DisableAPMode() error {
	return p.b.DisableAPMode()
}

func (p *Provisioner) ConnectToNetwork(ctx context.Context, ssid, psk string) (<-chan ProvisionUpdate, error) {
	if err := validateCredentials(ssid, psk); err != nil {
		return nil, err
	}
	return p.b.ConnectToNetwork(ctx, ssid, psk)
}

func (p *Provisioner) Close() error {
	return p.b.Close()
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
