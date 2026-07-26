package http

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dawsonalex/iotflow"
	"github.com/dawsonalex/iotflow/provision"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errFlow is a stubFlow whose Submit returns a canned error, so the credentials
// handler's error mapping can be exercised one branch at a time. Driving this
// through the real Flow isn't viable: Submit checks state before validating, so
// a Flow that hasn't Begun can only ever return ErrNotAwaitingCredentials, and
// ErrSubmissionPending is racy against a running state machine that drains the
// single-slot credentials channel immediately.
type errFlow struct {
	stubFlow
	submitErr error
}

func (f errFlow) Submit(_, _ string) error { return f.submitErr }

// TestCredentialStateStatusCode pins the contract of POST /credentials: every
// error the Flow can return maps to a specific HTTP status *and* a stable
// machine-readable code in the response body, and reaches the WithErrorHandler
// callback still classifiable with errors.Is. The codes are part of the public
// API — clients branch on them (retry vs. re-prompt the user) — so a rename
// here is a breaking change and must break this test.
func TestCredentialStateStatusCode(t *testing.T) {
	const validBody = `{"ssid":"home-net","psk":"good-password"}`

	cases := []struct {
		name       string
		body       string
		submitErr  error
		wantStatus int
		wantCode   string // "" means: no JSON error body expected
		wantErrIs  error  // error the WithErrorHandler callback must receive; nil means none
		// wantRedacted asserts the client is given a generic message and that
		// submitErr's text appears nowhere in the response.
		wantRedacted bool
	}{
		{
			name:       "accepted",
			body:       validBody,
			submitErr:  nil,
			wantStatus: http.StatusAccepted,
		},
		{
			// The Flow isn't in StateWaitingForCredentials. Retrying later may
			// work, so this is a conflict rather than a client error.
			name:       "not awaiting credentials",
			body:       validBody,
			submitErr:  iotflow.ErrNotAwaitingCredentials,
			wantStatus: http.StatusConflict,
			wantCode:   "ErrNotAwaitingCredentials",
			wantErrIs:  iotflow.ErrNotAwaitingCredentials,
		},
		{
			name:       "submission pending",
			body:       validBody,
			submitErr:  iotflow.ErrSubmissionPending,
			wantStatus: http.StatusConflict,
			wantCode:   "ErrSubmissionPending",
			wantErrIs:  iotflow.ErrSubmissionPending,
		},
		{
			// Both validation sentinels collapse to one client-facing code; the
			// specifics stay in the human-readable error string.
			name:       "invalid ssid",
			body:       validBody,
			submitErr:  provision.ErrSSIDInvalid,
			wantStatus: http.StatusBadRequest,
			wantCode:   "ErrInvalidCredentials",
			wantErrIs:  provision.ErrSSIDInvalid,
		},
		{
			name:       "invalid psk",
			body:       validBody,
			submitErr:  provision.ErrPSKInvalid,
			wantStatus: http.StatusBadRequest,
			wantCode:   "ErrInvalidCredentials",
			wantErrIs:  provision.ErrPSKInvalid,
		},
		{
			// The mapping is errors.Is-based, so a wrapped sentinel must classify
			// identically to a bare one — this guards against a future switch on
			// equality or on concrete types.
			name:       "wrapped sentinel",
			body:       validBody,
			submitErr:  fmt.Errorf("submitting credentials: %w", provision.ErrPSKInvalid),
			wantStatus: http.StatusBadRequest,
			wantCode:   "ErrInvalidCredentials",
			wantErrIs:  provision.ErrPSKInvalid,
		},
		{
			// Anything unrecognised is a server-side fault, not the client's —
			// and its text is not this package's to vouch for, so the client is
			// told nothing about it. The cause still reaches the error handler.
			name:         "unknown error",
			body:         validBody,
			submitErr:    errors.New("provisioner exploded"),
			wantStatus:   http.StatusInternalServerError,
			wantCode:     "credentials_error",
			wantRedacted: true,
			wantErrIs:    nil, // asserted against submitErr below
		},
		{
			// Rejected during decode, before the Flow is ever consulted, so the
			// outcome must not depend on submitErr. The callback sees an
			// ErrValidation-wrapped error, which lets an embedder filter client
			// mistakes out of its error logs.
			name:       "malformed json",
			body:       "{not json",
			submitErr:  iotflow.ErrSubmissionPending,
			wantStatus: http.StatusBadRequest,
			wantErrIs:  ErrValidation,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var gotErrs []error
			h := NewHandler(
				errFlow{submitErr: tc.submitErr},
				WithErrorHandler(func(_ *http.Request, err error) { gotErrs = append(gotErrs, err) }),
			)

			req := httptest.NewRequest(http.MethodPost, "/credentials", strings.NewReader(tc.body))
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			assert.Equal(t, tc.wantStatus, rec.Code)

			if tc.wantCode != "" {
				assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))

				var body errorResponse
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body), "response body: %s", rec.Body.String())
				assert.Equal(t, tc.wantCode, body.Code)
				if tc.wantRedacted {
					// An unrecognised error may carry backend detail, so the
					// client must be told nothing about it.
					assert.Equal(t, genericErrMsg, body.Error)
					assert.NotContains(t, rec.Body.String(), tc.submitErr.Error())
				} else {
					// Recognised errors are this package's own sentinels, so
					// their text is safe. Assert the cause is carried rather
					// than pinning exact wording.
					assert.Contains(t, body.Error, tc.submitErr.Error())
				}
			}

			switch {
			case tc.submitErr == nil:
				assert.Empty(t, gotErrs, "an accepted submission must not report an error")
			case tc.wantErrIs != nil:
				require.Len(t, gotErrs, 1)
				assert.ErrorIs(t, gotErrs[0], tc.wantErrIs)
			default:
				require.Len(t, gotErrs, 1)
				assert.ErrorIs(t, gotErrs[0], tc.submitErr)
			}
		})
	}
}
