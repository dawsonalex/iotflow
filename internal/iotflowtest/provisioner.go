// Package iotflowtest provides shared test doubles for exercising the iotflow
// package and the transports built on top of it. It lives under internal/ so it
// is importable anywhere in this module but stays out of the public API.
//
// The file is deliberately a regular .go source file (not *_test.go): a _test.go
// file is compiled only into its own package's test binary and cannot be
// imported by other packages, which would defeat the point of sharing it.
package iotflowtest

import (
	"context"

	"github.com/dawsonalex/iotflow/provision"
)

// Compile-time assertion that the mock satisfies the interface it stands in for.
var _ provision.Provisioner = (*MockProvisioner)(nil)

// MockProvisioner is a configurable provisioner.Provisioner for tests. Each method
// delegates to the corresponding exported function field, so a test sets only
// the behavior it cares about. A nil field for IsConnected/EnableAPMode/
// ConnectToNetwork panics when called — an intentional signal that the test
// reached a step it never stubbed. DisableAPMode and Close default to no-ops,
// so tests that don't reach those steps can leave them unset.
//
// The fields are exported because struct-literal construction from another
// package (e.g. transport/http tests) requires it.
type MockProvisioner struct {
	IsConnectedFn      func(context.Context) (bool, error)
	EnableAPModeFn     func(context.Context, string, string) (<-chan provision.Update, error)
	DisableAPModeFn    func(context.Context) error
	ConnectToNetworkFn func(context.Context, string, string) (<-chan provision.Update, error)
	ScanFn             func(context.Context) ([]provision.Network, error)
	CloseFn            func() error
}

func (m *MockProvisioner) Scan(ctx context.Context) ([]provision.Network, error) {
	return m.ScanFn(ctx)
}

func (m *MockProvisioner) IsConnected(ctx context.Context) (bool, error) {
	return m.IsConnectedFn(ctx)
}

func (m *MockProvisioner) EnableAPMode(ctx context.Context, ssid, psk string) (<-chan provision.Update, error) {
	return m.EnableAPModeFn(ctx, ssid, psk)
}

func (m *MockProvisioner) DisableAPMode(ctx context.Context) error {
	if m.DisableAPModeFn != nil {
		return m.DisableAPModeFn(ctx)
	}
	return nil
}

func (m *MockProvisioner) ConnectToNetwork(ctx context.Context, ssid, psk string) (<-chan provision.Update, error) {
	return m.ConnectToNetworkFn(ctx, ssid, psk)
}

func (m *MockProvisioner) Close() error {
	if m.CloseFn != nil {
		return m.CloseFn()
	}
	return nil
}

// ConnectedCh returns a closed channel carrying a single Connected update — the
// common "this step succeeded immediately" stub for EnableAPMode/ConnectToNetwork.
func ConnectedCh() <-chan provision.Update {
	ch := make(chan provision.Update, 1)
	ch <- provision.Update{State: provision.StateConnected}
	close(ch)
	return ch
}

// FailedCh returns a closed channel carrying a single Failed update wrapping err.
func FailedCh(err error) <-chan provision.Update {
	ch := make(chan provision.Update, 1)
	ch <- provision.Update{State: provision.StateFailed, Err: err}
	close(ch)
	return ch
}
