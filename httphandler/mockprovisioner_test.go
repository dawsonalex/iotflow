package httphandler

import (
	"context"

	"github.com/dawsonalex/iotflow"
)

var _ HandlerProvisioner = &mockProvisioner{}

type mockProvisioner struct {
	isConnected    bool
	isConnectedErr error

	enableAPModeUpdates []iotflow.ProvisionUpdate
	enableAPModeErr     error

	disableAPModeErr error

	connectToNetworkUpdates []iotflow.ProvisionUpdate
	connectToNetworkErr     error
}

func (m *mockProvisioner) IsConnected(ctx context.Context) (bool, error) {
	return m.isConnected, m.isConnectedErr
}

func (m *mockProvisioner) EnableAPMode(ctx context.Context, ssid, psk string) (<-chan iotflow.ProvisionUpdate, error) {
	if m.enableAPModeErr != nil {
		return nil, m.enableAPModeErr
	}
	ch := make(chan iotflow.ProvisionUpdate, len(m.enableAPModeUpdates))
	for _, u := range m.enableAPModeUpdates {
		ch <- u
	}
	close(ch)
	return ch, nil
}

func (m *mockProvisioner) DisableAPMode() error {
	return m.disableAPModeErr
}

func (m *mockProvisioner) ConnectToNetwork(ctx context.Context, ssid, psk string) (<-chan iotflow.ProvisionUpdate, error) {
	if m.connectToNetworkErr != nil {
		return nil, m.connectToNetworkErr
	}
	ch := make(chan iotflow.ProvisionUpdate, len(m.connectToNetworkUpdates))
	for _, u := range m.connectToNetworkUpdates {
		ch <- u
	}
	close(ch)
	return ch, nil
}
