package httphandler

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHandlerStatus(t *testing.T) {
	p := &mockProvisioner{}
	h := New(p)

	s := httptest.NewServer(h)
	defer s.Close()

	req, err := http.NewRequestWithContext(t.Context(), "GET", s.URL+"/status", nil)
	assert.Nil(t, err)
	resp, err := s.Client().Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	status := &statusResponse{}
	err = json.NewDecoder(resp.Body).Decode(status)
	assert.Nil(t, err)
	assert.Equal(t, status.Connected, false)

	p.isConnected = true
	req, err = http.NewRequestWithContext(t.Context(), "GET", s.URL+"/status", nil)
	assert.Nil(t, err)
	resp, err = s.Client().Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	err = json.NewDecoder(resp.Body).Decode(status)
	assert.Nil(t, err)
	assert.Equal(t, status.Connected, true)
}

func TestHandlerStatusError(t *testing.T) {
	isConnectedErr := errors.New("backend error")
	p := &mockProvisioner{
		isConnectedErr: isConnectedErr,
	}

	errorHandlerRan := atomic.Bool{}
	h := New(p, WithErrorHandler(func(r *http.Request, err error) {
		errorHandlerRan.Store(true)
	}))
	s := httptest.NewServer(h)
	defer s.Close()

	req, err := http.NewRequestWithContext(t.Context(), "GET", s.URL+"/status", nil)
	assert.Nil(t, err)
	resp, err := s.Client().Do(req)
	assert.Nil(t, err)

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	errResponse := &errorResponse{}
	err = json.NewDecoder(resp.Body).Decode(errResponse)
	assert.Nil(t, err)

	expectedErr := errorResponse{
		isConnectedErr.Error(),
	}

	// assert error format and handler ran.
	assert.Equal(t, expectedErr, *errResponse)
	assert.True(t, errorHandlerRan.Load())
}
