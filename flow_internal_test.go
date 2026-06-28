package iotflow

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

// closeStub is a minimal Provisioner used to exercise Finish's ownership branch.
// It lives in the internal (package iotflow) test because that branch is set
// only via the unexported ownsProvisioner field — and because importing the
// shared iotflowtest mock here would create an import cycle (it imports iotflow).
// Only Close is meaningful; the other methods are never reached by Finish and
// panic if they somehow are.
type closeStub struct {
	closeErr   error
	closeCalls int
}

func (s *closeStub) IsConnected(context.Context) (bool, error) { panic("unused") }
func (s *closeStub) EnableAPMode(context.Context, string, string) (<-chan ProvisionUpdate, error) {
	panic("unused")
}
func (s *closeStub) DisableAPMode() error { panic("unused") }
func (s *closeStub) ConnectToNetwork(context.Context, string, string) (<-chan ProvisionUpdate, error) {
	panic("unused")
}
func (s *closeStub) Close() error {
	s.closeCalls++
	return s.closeErr
}

func TestFinish_OwnedProvisionerIsClosed(t *testing.T) {
	p := &closeStub{}
	// Mirrors what NewNetworkManagerFlow sets up: the Flow owns the backend.
	f := &Flow{provisioner: p, ownsProvisioner: true}

	assert.NoError(t, f.Finish())
	assert.Equal(t, 1, p.closeCalls, "Finish must close a Provisioner the Flow owns")
}

func TestFinish_OwnedProvisionerCloseErrorPropagates(t *testing.T) {
	wantErr := errors.New("close failed")
	p := &closeStub{closeErr: wantErr}
	f := &Flow{provisioner: p, ownsProvisioner: true}

	assert.ErrorIs(t, f.Finish(), wantErr)
}
