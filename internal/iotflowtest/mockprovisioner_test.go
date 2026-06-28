package iotflowtest

import (
	"context"

	"github.com/dawsonalex/iotflow"
)

// MockProvisioner implements Backend for testing. Nil function fields panic if
// called — an intentional signal that the test made an unexpected call.
// DisableAPMode and Close default to no-ops, so tests that don't reach those
// steps don't have to stub them.
type MockProvisioner struct {
	isConnectedFn      func(context.Context) (bool, error)
	enableAPModeFn     func(context.Context, string, string) (<-chan iotflow.ProvisionUpdate, error)
	disableAPModeFn    func() error
	connectToNetworkFn func(context.Context, string, string) (<-chan iotflow.ProvisionUpdate, error)
}

func (m *MockProvisioner) IsConnected(ctx context.Context) (bool, error) {
	return m.isConnectedFn(ctx)
}

func (m *MockProvisioner) EnableAPMode(ctx context.Context, ssid, psk string) (<-chan iotflow.ProvisionUpdate, error) {
	return m.enableAPModeFn(ctx, ssid, psk)
}

func (m *MockProvisioner) DisableAPMode() error {
	if m.disableAPModeFn != nil {
		return m.disableAPModeFn()
	}
	return nil
}

func (m *MockProvisioner) ConnectToNetwork(ctx context.Context, ssid, psk string) (<-chan iotflow.ProvisionUpdate, error) {
	return m.connectToNetworkFn(ctx, ssid, psk)
}

func (m *MockProvisioner) Close() error { return nil }
