# iotflow

A Go library for WiFi provisioning on Linux IoT devices using a Soft AP flow.

The typical problem: a device ships with no network credentials. Rather than requiring SSH access or a config file, 
iotflow puts the device into access point mode so a user can connect to it directly and supply credentials via a web interface. 
Once credentials are received, the device switches to station mode and joins the target network.

```
Device boots, no network credentials
        │
        ▼
  Check if connected ──── yes ──► Normal operation
        │ no
        ▼
  Enable AP mode (device becomes an access point)
        │
        ▼
  User connects and submits WiFi credentials
        │
        ▼
  Disable AP mode, connect to target network
        │
        ├──── failed ──► re-enable AP, wait for new credentials
        ▼
  Normal operation
```

## Requirements

- Linux
- [NetworkManager](https://networkmanager.dev/) (the current backend)
- Elevated privileges or a [PolicyKit rule](#permissions) allowing network management

## Installation

```sh
go get github.com/dawsonalex/iotflow
```

## Concepts

iotflow is built from two layers that you compose:

- **`Provisioner`** — a backend interface that performs the low-level network
  operations (enable AP mode, connect to a network, etc.). The library ships
  with `NetworkManagerProvisioner`, which talks to NetworkManager over D-Bus.
- **`Flow`** — a transport-agnostic state machine that drives the full
  provisioning lifecycle on top of a `Provisioner`: check connectivity, bring up
  the AP, wait for station credentials, tear the AP down, and connect. If the
  connection attempt fails it loops back into AP mode so the user can try again.

## Usage

Cnstruct a `Provisioner` and pass it to `NewFlow`. You must call `Close()` on the
`Provisioner` to clean up it's resources when the flow is complete.

```go
func main() {
    p, err := networkManager.NewProvisioner("wlan0")
    if err != nil {
        log.Fatal(err)
    }
    defer p.Close() // you created the provisioner, so you close it

    // NewFlow does not take ownership of p; Finish leaves it untouched.
    f, err := iotflow.NewFlow("iotflow-setup", "setup-password", p)
    if err != nil {
        log.Fatal(err)
    }
    defer f.Finish()

    // Call Subscribe() to observe changes in state
	for update := range f.Subscribe() {
        fmt.Printf("state: %v\n", update.State)
    }
	
	// Call Begin() to start the flow
	if err := f.Begin(context.Background()); err != nil {
		log.Fatal(err)
	}
}
```

### Exposing a Flow over HTTP

The `transport/http` subpackage runs an HTTP server alongside a `Flow`. It serves
three endpoints:

- `POST /credentials` - accepts `{"ssid": "...", "psk": "..."}` and forwards it
  to `Flow.Submit`. Returns `202 Accepted` on success, `400` for invalid
  credentials, `409` if a submission is already pending or the flow is no longer awaiting credentials (indicated with a specific error).
- `GET /events` - a Server-Sent Events stream of `FlowUpdate`s as they happen.
- `GET /aps` - A list of the APs visisble to the device.

```go
package main

import (
    "context"
    "log"
    "net/http"

    "github.com/dawsonalex/iotflow"
    iothttp "github.com/dawsonalex/iotflow/transport/http"
)

func main() {
	p, err := networkmanager.NewProvisioner("wlan0")
	if err != nil {
		log.Fatal(err)
	}
    defer p.Close()
	
    f, err := iotflow.NewNetworkManagerFlow("iotflow-setup", "setup-password", p)
    if err != nil {
        log.Fatal(err)
    }
    defer f.Finish()

    // Serve runs the HTTP server and the Flow together, returning when the
    // first of them finishes or ctx is cancelled. It binds :80 by default;
    // use WithAddress / WithListener to change that.
    err = iothttp.Serve(context.Background(), f,
        iothttp.WithAddress(":8080"),
        iothttp.WithErrorHandler(func(r *http.Request, err error) {
            log.Printf("http: %v", err)
        }),
    )
    if err != nil {
        log.Fatal(err)
    }
}
```

### Embedding the endpoints in your own server

If you'd rather mount the provisioning endpoints into a server you already run,
use `NewHandler(f, opts...)`. It returns an `http.Handler` for the endpoints
above without starting a listener.

```go
func provisioningRoutes(f *iotflow.Flow) http.Handler {
    // Serve runs the lifecycle for you; here it's your job. Cancel this ctx to
    // stop the Flow, and Finish it when you're done (below).
    go func() {
        if err := f.Begin(context.Background()); err != nil {
            log.Printf("provisioning: %v", err)
        }
    }()

    mux := http.NewServeMux()
    mux.Handle("/api/", myAppHandler())

    // WithPrefix bakes the mount path into the endpoint routes so the parent
    // mount lines up without http.StripPrefix. The endpoints land at
    // /provision/credentials and /provision/events.
    mux.Handle("/provision/", iothttp.NewHandler(f, iothttp.WithPrefix("/provision")))
    return mux
}
```

Remember to `defer f.Finish()` wherever you own the `Flow`; it closes the backend
only if the `Flow` created it (via `NewNetworkManagerFlow`).

### Reading state updates

`Subscribe` returns a `<-chan FlowUpdate`. Multiple subscribers may observe
concurrently; each channel is buffered, and a subscriber that falls behind has
updates dropped rather than blocking the state machine. Every subscription is
closed when `Begin` returns — subscribing afterwards yields an already-closed
channel. Call `Unsubscribe` to release one early.

```go
type FlowUpdate struct {
    State  FlowState     // see below
    Err    error         // raw backend error; non-nil on Failed and AttemptFailed
    Reason FailureReason // classification of Err, safe to put on the wire
}
```

`FlowState` transitions through: `Idle` → `CheckingConnection` → `EnablingAP` →
`WaitingForCredentials` → `DisablingAP` → `Connecting`, ending in one of the
terminal states `Provisioned` or `Failed`. `Provisioned` means the device is on
a network, whether it was already online at `Begin` or reached it through the
full flow — a subscriber that needs to tell those apart can look at whether
`EnablingAP` was ever emitted.

### Recoverable failures

Not every failure is terminal. If disabling AP mode or connecting to the target
network fails, the `Flow` emits `AttemptFailed` and then returns to `EnablingAP`
→ `WaitingForCredentials` so the user can submit again:

```
… → Connecting   → AttemptFailed → EnablingAP → WaitingForCredentials → …
… → DisablingAP  → AttemptFailed → EnablingAP → WaitingForCredentials → …
```

`AttemptFailed` is the only non-terminal state that carries an error, and it is
*emitted rather than occupied* — the update immediately after it names the state
the machine actually moved to, so a client switching on state never has to
handle it as a resting place. Use it to tell the user why the setup page came
back rather than leaving them to guess:

| `Reason`                 | JSON               | Means                                        |
| ------------------------ | ------------------ | -------------------------------------------- |
| `ReasonConnectFailed`    | `connect_failed`   | `ConnectToNetwork` failed — wrong PSK, wrong SSID, network out of range |
| `ReasonAPTeardownFailed` | `ap_teardown_failed` | `DisableAPMode` failed — a device-side problem, but resubmitting is still worth a try |
| `ReasonInternal`         | `internal`         | anything else, including every `Failed`       |

Retries are unbounded but each one waits on a fresh credential submission, so
this cannot spin: a client that never resubmits simply parks in
`WaitingForCredentials`.

### Error redaction

`FlowUpdate` marshals to JSON as
`{"state": "...", "reason": "...", "error": "..."}`. The `error` field is
**`Reason.Message()`, never `Err.Error()`**.

This is deliberate. The far end of a provisioning stream is an unauthenticated
client sitting on the device's setup AP, and backend errors carry D-Bus object
paths, interface names and NetworkManager internals. Because the redaction lives
in `MarshalJSON` rather than in one transport, a custom BLE/MQTT/HTTP transport
that marshals a `FlowUpdate` gets the safe encoding for free.

Go consumers that want the underlying error read `Err` off the struct directly;
`errors.Is` works as normal. That is the right channel for device logs.

The same rule applies to the other endpoints in `transport/http`: `GET /networks`
and `POST /credentials` report only errors this module defines itself. Anything
unrecognised — which is where backend text would arrive — becomes a generic
message, with the real error going to `WithErrorHandler`. **If you write your own
transport, apply the same rule**: `MarshalJSON` protects `FlowUpdate`, but any
error you surface yourself is yours to redact.

## API

### Backend

```
networkmanager.NewProvisioner(iface string) (*networkmanager.Provisioner, error)

type Provisioner interface {
    IsConnected(ctx context.Context) (bool, error)
    EnableAPMode(ctx context.Context, ssid, psk string) (<-chan provision.Update, error)
    DisableAPMode(ctx context.Context) error
    ConnectToNetwork(ctx context.Context, ssid, psk string) (<-chan provision.Update, error)
    Close() error
    Scan(context.Context) ([]Network, error)
}
```

`iface == ""` auto-discovers the first WiFi device; otherwise the named
interface is looked up and verified to be a WiFi adapter.

### Flow

```
NewNetworkManagerFlow(apSSID, apPSK, iface string, opts ...FlowOpt) (*Flow, error) // managed backend; Finish closes it
NewFlow(apSSID, apPSK string, p Provisioner, opts ...FlowOpt) (*Flow, error)       // bring your own Provisioner; you close it

(*Flow).Begin(ctx context.Context) error       // runs the lifecycle; call once, blocks until terminal
(*Flow).Submit(ssid, psk string) error         // hand station credentials to the running Flow
(*Flow).Subscribe() <-chan FlowUpdate           // observe state transitions
(*Flow).Unsubscribe(ch <-chan FlowUpdate)       // release a subscription early
(*Flow).Finish() error                          // tear down; closes the backend only if the Flow created it
```

`apSSID`/`apPSK` are the credentials for the device's own access point and are
validated up front. `Finish` closes the backend only when the `Flow` created it
(via `NewNetworkManagerFlow`); a `Provisioner` you passed to `NewFlow` remains
yours to close.

### HTTP transport (`transport/http`)

```
Serve(ctx context.Context, f *iotflow.Flow, opts ...HandlerOpt) error
NewHandler(f Flow, opts ...HandlerOpt) http.Handler

WithAddress(addr string) HandlerOpt              // default ":80" (Serve only)
WithListener(ln net.Listener) HandlerOpt         // supply a pre-bound listener (Serve only)
WithPrefix(prefix string) HandlerOpt             // path prefix for mounted routes (normalized); default "" (flat)
WithKeepalive(d time.Duration) HandlerOpt        // SSE heartbeat interval; default 15s, non-positive disables
WithErrorHandler(func(r *http.Request, err error)) HandlerOpt
```

### Errors

```
ErrSSIDInvalid       // SSID must be 1–32 characters
ErrPSKInvalid        // PSK must be 8–63 characters
ErrSubmissionPending // a previous Submit has not yet been consumed

ErrNotAwaitingCredentials // The flow is not in a state to accept credentials. The submitted credentials will be discarded.
ErrUpdateChanClosedPrematurely // The Flow state update channel has been closed before the Flow reaches a terminal state.
```

**Credential constraints** (enforced by `Submit` and by the backend before any
network call):
- SSID: 1–32 characters
- PSK: 8–63 characters

## Permissions

Network operations require permission to talk to NetworkManager over D-Bus. The simplest approach during development is `sudo`. For production, create a PolicyKit rule that grants your service account the required permissions without running as root:

```
/etc/polkit-1/rules.d/99-iotflow.rules
```

```js
polkit.addRule(function(action, subject) {
    if (action.id.indexOf("org.freedesktop.NetworkManager.") === 0 &&
        subject.user === "your-service-user") {
        return polkit.Result.YES;
    }
});
```

## Extending with a custom backend

The NetworkManager implementation is one backend. To support a different system
(e.g. `wpa_supplicant`), implement the exported `Provisioner` interface and pass
it to `NewFlow`. Because you constructed the backend, you own its lifecycle — the
`Flow` will not close it for you:

```go
p := myCustomBackend{}
f, err := iotflow.NewFlow("iotflow-setup", "setup-password", p)
// ... if p needs cleanup, defer p.Close() yourself.
```

The `Flow` type and all provisioning logic are backend-agnostic.
