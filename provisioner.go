package iotflow

import "context"

type Provisioner interface {
	IsConnected(context.Context) (bool, error)
	EnableAPMode(context.Context, string, string) (<-chan ProvisionUpdate, error)
	DisableAPMode() error
	ConnectToNetwork(context.Context, string, string) (<-chan ProvisionUpdate, error)
	Close() error
}
