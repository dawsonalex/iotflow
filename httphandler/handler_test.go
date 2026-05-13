package httphandler

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/dawsonalex/iotflow"
	"github.com/stretchr/testify/assert"
)

// readSSEEvents reads all "data: ..." lines from an SSE response body,
// returning the payload string of each event.
func readSSEEvents(t *testing.T, body io.ReadCloser) []string {
	t.Helper()
	var events []string
	scanner := bufio.NewScanner(body)
	for scanner.Scan() {
		if data, ok := strings.CutPrefix(scanner.Text(), "data: "); ok {
			events = append(events, data)
		}
	}
	assert.NoError(t, scanner.Err())
	return events
}

// GET /status

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

func TestHandlerStatusNoErrorHandler(t *testing.T) {
	p := &mockProvisioner{isConnectedErr: errors.New("backend error")}
	h := New(p)
	s := httptest.NewServer(h)
	defer s.Close()

	req, err := http.NewRequestWithContext(t.Context(), "GET", s.URL+"/status", nil)
	assert.Nil(t, err)
	resp, err := s.Client().Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

// POST /ap

func TestHandlerEnableApMode(t *testing.T) {
	updates := []iotflow.ProvisionUpdate{
		{State: iotflow.ProvisionStateConnecting},
		{State: iotflow.ProvisionStateConnected},
	}
	p := &mockProvisioner{enableAPModeUpdates: updates}
	h := New(p)
	s := httptest.NewServer(h)
	defer s.Close()

	req, err := http.NewRequestWithContext(t.Context(), "POST", s.URL+"/ap",
		strings.NewReader(`{"ssid":"TestNet","psk":"password123"}`))
	assert.Nil(t, err)
	resp, err := s.Client().Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))
	assert.Equal(t, "no-cache", resp.Header.Get("Cache-Control"))

	events := readSSEEvents(t, resp.Body)
	assert.Len(t, events, 2)

	type stateOnly struct {
		State iotflow.ProvisionState `json:"State"`
	}
	var first, second stateOnly
	assert.NoError(t, json.Unmarshal([]byte(events[0]), &first))
	assert.NoError(t, json.Unmarshal([]byte(events[1]), &second))
	assert.Equal(t, iotflow.ProvisionStateConnecting, first.State)
	assert.Equal(t, iotflow.ProvisionStateConnected, second.State)
}

func TestHandlerEnableApModeInvalidBody(t *testing.T) {
	errCh := make(chan error, 1)
	p := &mockProvisioner{}
	h := New(p, WithErrorHandler(func(r *http.Request, err error) {
		errCh <- err
	}))
	s := httptest.NewServer(h)
	defer s.Close()

	req, err := http.NewRequestWithContext(t.Context(), "POST", s.URL+"/ap",
		strings.NewReader("not json"))
	assert.Nil(t, err)
	resp, err := s.Client().Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	select {
	case handlerErr := <-errCh:
		assert.True(t, errors.Is(handlerErr, ValidationError))
	default:
		t.Error("onError was not called")
	}
}

func TestHandlerEnableApModeValidationError(t *testing.T) {
	errCh := make(chan error, 1)
	p := &mockProvisioner{enableAPModeErr: iotflow.ErrSSIDInvalid}
	h := New(p, WithErrorHandler(func(r *http.Request, err error) {
		errCh <- err
	}))
	s := httptest.NewServer(h)
	defer s.Close()

	req, err := http.NewRequestWithContext(t.Context(), "POST", s.URL+"/ap",
		strings.NewReader(`{"ssid":"TestNet","psk":"password123"}`))
	assert.Nil(t, err)
	resp, err := s.Client().Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	select {
	case handlerErr := <-errCh:
		assert.True(t, errors.Is(handlerErr, ValidationError))
	default:
		t.Error("onError was not called")
	}
}

func TestHandlerEnableApModeError(t *testing.T) {
	backendErr := errors.New("backend error")
	errCh := make(chan error, 1)
	p := &mockProvisioner{enableAPModeErr: backendErr}
	h := New(p, WithErrorHandler(func(r *http.Request, err error) {
		errCh <- err
	}))
	s := httptest.NewServer(h)
	defer s.Close()

	req, err := http.NewRequestWithContext(t.Context(), "POST", s.URL+"/ap",
		strings.NewReader(`{"ssid":"TestNet","psk":"password123"}`))
	assert.Nil(t, err)
	resp, err := s.Client().Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	select {
	case handlerErr := <-errCh:
		assert.True(t, errors.Is(handlerErr, backendErr))
	default:
		t.Error("onError was not called")
	}
}

// DELETE /ap

func TestHandlerDisableApMode(t *testing.T) {
	p := &mockProvisioner{}
	h := New(p)
	s := httptest.NewServer(h)
	defer s.Close()

	req, err := http.NewRequestWithContext(t.Context(), "DELETE", s.URL+"/ap", nil)
	assert.Nil(t, err)
	resp, err := s.Client().Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
}

func TestHandlerDisableApModeError(t *testing.T) {
	backendErr := errors.New("backend error")
	errCh := make(chan error, 1)
	p := &mockProvisioner{disableAPModeErr: backendErr}
	h := New(p, WithErrorHandler(func(r *http.Request, err error) {
		errCh <- err
	}))
	s := httptest.NewServer(h)
	defer s.Close()

	req, err := http.NewRequestWithContext(t.Context(), "DELETE", s.URL+"/ap", nil)
	assert.Nil(t, err)
	resp, err := s.Client().Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	select {
	case handlerErr := <-errCh:
		assert.True(t, errors.Is(handlerErr, backendErr))
	default:
		t.Error("onError was not called")
	}
}

func TestHandlerDisableApModeNoErrorHandler(t *testing.T) {
	p := &mockProvisioner{disableAPModeErr: errors.New("backend error")}
	h := New(p)
	s := httptest.NewServer(h)
	defer s.Close()

	req, err := http.NewRequestWithContext(t.Context(), "DELETE", s.URL+"/ap", nil)
	assert.Nil(t, err)
	resp, err := s.Client().Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

// POST /connect

func TestHandlerConnectToNetwork(t *testing.T) {
	updates := []iotflow.ProvisionUpdate{
		{State: iotflow.ProvisionStateConnecting},
		{State: iotflow.ProvisionStateConnected},
	}
	p := &mockProvisioner{connectToNetworkUpdates: updates}
	h := New(p)
	s := httptest.NewServer(h)
	defer s.Close()

	req, err := http.NewRequestWithContext(t.Context(), "POST", s.URL+"/connect",
		strings.NewReader(`{"ssid":"TestNet","psk":"password123"}`))
	assert.Nil(t, err)
	resp, err := s.Client().Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))
	assert.Equal(t, "no-cache", resp.Header.Get("Cache-Control"))

	events := readSSEEvents(t, resp.Body)
	assert.Len(t, events, 2)

	type stateOnly struct {
		State iotflow.ProvisionState `json:"State"`
	}
	var first, second stateOnly
	assert.NoError(t, json.Unmarshal([]byte(events[0]), &first))
	assert.NoError(t, json.Unmarshal([]byte(events[1]), &second))
	assert.Equal(t, iotflow.ProvisionStateConnecting, first.State)
	assert.Equal(t, iotflow.ProvisionStateConnected, second.State)
}

func TestHandlerConnectToNetworkInvalidBody(t *testing.T) {
	errCh := make(chan error, 1)
	p := &mockProvisioner{}
	h := New(p, WithErrorHandler(func(r *http.Request, err error) {
		errCh <- err
	}))
	s := httptest.NewServer(h)
	defer s.Close()

	req, err := http.NewRequestWithContext(t.Context(), "POST", s.URL+"/connect",
		strings.NewReader("not json"))
	assert.Nil(t, err)
	resp, err := s.Client().Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	select {
	case handlerErr := <-errCh:
		assert.True(t, errors.Is(handlerErr, ValidationError))
	default:
		t.Error("onError was not called")
	}
}

func TestHandlerConnectToNetworkValidationError(t *testing.T) {
	errCh := make(chan error, 1)
	p := &mockProvisioner{connectToNetworkErr: iotflow.ErrPSKInvalid}
	h := New(p, WithErrorHandler(func(r *http.Request, err error) {
		errCh <- err
	}))
	s := httptest.NewServer(h)
	defer s.Close()

	req, err := http.NewRequestWithContext(t.Context(), "POST", s.URL+"/connect",
		strings.NewReader(`{"ssid":"TestNet","psk":"password123"}`))
	assert.Nil(t, err)
	resp, err := s.Client().Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	select {
	case handlerErr := <-errCh:
		assert.True(t, errors.Is(handlerErr, ValidationError))
	default:
		t.Error("onError was not called")
	}
}

func TestHandlerConnectToNetworkError(t *testing.T) {
	backendErr := errors.New("backend error")
	errCh := make(chan error, 1)
	p := &mockProvisioner{connectToNetworkErr: backendErr}
	h := New(p, WithErrorHandler(func(r *http.Request, err error) {
		errCh <- err
	}))
	s := httptest.NewServer(h)
	defer s.Close()

	req, err := http.NewRequestWithContext(t.Context(), "POST", s.URL+"/connect",
		strings.NewReader(`{"ssid":"TestNet","psk":"password123"}`))
	assert.Nil(t, err)
	resp, err := s.Client().Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	select {
	case handlerErr := <-errCh:
		assert.True(t, errors.Is(handlerErr, backendErr))
	default:
		t.Error("onError was not called")
	}
}
