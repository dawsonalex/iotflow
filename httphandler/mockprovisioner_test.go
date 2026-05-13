package httphandler

import (
	"context"

	"github.com/dawsonalex/iotflow"
)

var _ HandlerProvisioner = &mockProvisioner{}

type mockProvisioner struct {
	isConnected    bool
	isConnectedErr error
}

func (m *mockProvisioner) IsConnected(ctx context.Context) (bool, error) {
	return m.isConnected, m.isConnectedErr
}

func (m *mockProvisioner) EnableAPMode(ctx context.Context, ssid, psk string) (<-chan iotflow.ProvisionUpdate, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mockProvisioner) DisableAPMode() error {
	//TODO implement me
	panic("implement me")
}

func (m *mockProvisioner) ConnectToNetwork(ctx context.Context, ssid, psk string) (<-chan iotflow.ProvisionUpdate, error) {
	//TODO implement me
	panic("implement me")
}
