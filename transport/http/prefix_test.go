package http

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dawsonalex/iotflow"
	"github.com/stretchr/testify/assert"
)

// stubFlow is a minimal Flow implementation for handler-routing tests. It
// doesn't run a state machine — these tests only care about whether a request
// reaches the credentials handler, not what the Flow does with it. Submit always
// accepts, so a routed POST /credentials yields 202; a request that never
// reaches the handler yields 404 from the mux instead.
type stubFlow struct{}

func (stubFlow) ListAccessPoints(ctx context.Context) ([]iotflow.Network, error) {
	panic("unimplemented")
}

func (stubFlow) Submit(_, _ string) error { return nil }

func (stubFlow) Subscribe() (<-chan iotflow.FlowUpdate, func()) {
	ch := make(chan iotflow.FlowUpdate)
	return ch, func() { close(ch) }
}

// TestPrefixMountRoutesWithoutStripPrefix pins the embedding contract: a caller
// can mount NewHandler under a path prefix in their own mux and have the
// endpoints resolve — without wrapping it in http.StripPrefix.
//
// This is the regression test for the prefix fix. It exercises WithPrefix, so
// until that option exists the package won't compile — that is the current
// breakage. Once WithPrefix bakes the mount path into the registered patterns,
// the prefixed case resolves to the handler (202) instead of falling through to
// the mux's 404.
//
// The empty-prefix row guards the existing flat behavior: WithPrefix("") must
// leave today's `POST /credentials` / `GET /events` patterns unchanged. The
// remaining rows guard prefix normalization (leading/trailing slashes).
func TestPrefixMountRoutesWithoutStripPrefix(t *testing.T) {
	cases := []struct {
		name   string
		prefix string
		mount  string // parent pattern the handler is mounted under
		path   string // request path
	}{
		{name: "flat", prefix: "", mount: "/", path: "/credentials"},
		{name: "flat", prefix: "/", mount: "/", path: "/credentials"},
		{name: "prefixed", prefix: "/provision", mount: "/provision/", path: "/provision/credentials"},

		// Normalization: a caller may spell the prefix without a leading slash
		// or with a trailing one. Both must resolve to the same routes as
		// "/provision". Without normalization the first silently 404s (the bare
		// word is parsed as a host) and the second panics in mux() on a "//"
		// pattern — so these rows guard WithPrefix's normalization.
		{name: "no leading slash", prefix: "provision", mount: "/provision/", path: "/provision/credentials"},
		{name: "trailing slash", prefix: "/provision/", mount: "/provision/", path: "/provision/credentials"},
		{name: "double slash internal", prefix: "/a//b", mount: "/a/b/", path: "/a/b/credentials"},
		//{name: "including ..", prefix: "/a/../b", mount: "/a/b/", path: "/a/b/credentials"}, // removed this case, it should always panic
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parent := http.NewServeMux()
			parent.Handle(tc.mount, NewHandler(stubFlow{}, WithPrefix(tc.prefix)))

			body := strings.NewReader(`{"ssid":"net","psk":"password"}`)
			req := httptest.NewRequest(http.MethodPost, tc.path, body)
			rec := httptest.NewRecorder()

			parent.ServeHTTP(rec, req)

			// 404 here means the request never reached the credentials handler —
			// i.e. the prefix didn't line up (the breakage this test guards).
			assert.Equal(t, http.StatusAccepted, rec.Code)
		})
	}
}

// ExampleWithPrefix mounts the provisioning endpoints under a path prefix in a
// parent mux. WithPrefix bakes the prefix into the routes, so the
// parent mount resolves without having to use http.StripPrefix.
func ExampleWithPrefix() {
	app := http.NewServeMux()
	app.Handle("/provision/", NewHandler(stubFlow{}, WithPrefix("/provision")))

	// A POST that a client of the embedded server would make.
	body := strings.NewReader(`{"ssid":"net","psk":"password"}`)
	req := httptest.NewRequest(http.MethodPost, "/provision/credentials", body)
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, req)

	fmt.Println(rec.Code) // 202 Accepted — routed to the credentials handler

	// Output:
	// 202
}
