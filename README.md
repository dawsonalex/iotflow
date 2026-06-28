# iotflow

A Go library for WiFi provisioning on Linux IoT devices using the Soft AP flow.

The typical problem: a device ships with no network credentials. Rather than requiring SSH access or a config file, iotflow puts the device into access point mode so a user can connect to it directly and supply credentials via a web interface. Once credentials are received, the device switches to station mode and joins the target network.

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

A `Flow` knows nothing about *how* credentials reach it or *how* progress is
reported. Credentials arrive through `Submit` and progress is observed through
`Subscribe`; a transport adapts those two seams to a wire protocol. The
`transport/http` subpackage is one such transport (HTTP + Server-Sent Events).

## Usage

### Driving a Flow directly

```go
package main

import (
    "context"
    "log"

    "github.com/dawsonalex/iotflow"
)

func main() {
    // The NetworkManager backend. Pass "" to auto-discover the first WiFi
    // device, or an interface name (e.g. "wlan0") to pin to a specific adapter.
    p, err := iotflow.NewNetworkManagerProvisioner("wlan0")
    if err != nil {
        log.Fatal(err)
    }
    defer p.Close()

    // A Flow orchestrates the whole lifecycle on top of the backend.
    f, err := iotflow.NewFlow(p)
    if err != nil {
        log.Fatal(err)
    }
    defer f.Finish()

    // Observe state transitions. Subscribe before Begin so no updates are missed.
    go func() {
        for upd := range f.Subscribe() {
            if upd.Err != nil {
                log.Printf("flow error: %v", upd.Err)
                continue
            }
            log.Printf("state: %s", upd.State)
        }
    }()

    // Feed station credentials in once you have them (e.g. from your own
    // transport). Submit is non-blocking and safe for concurrent use.
    go func() {
        ssid, psk := receiveCredentials()
        if err := f.Submit(ssid, psk); err != nil {
            log.Printf("submit rejected: %v", err)
        }
    }()

    // Begin blocks until the device is connected or an unrecoverable error
    // occurs. It must be called exactly once per Flow.
    if err := f.Begin(context.Background()); err != nil {
        log.Fatalf("provisioning failed: %v", err)
    }
    log.Println("provisioned")
}
```

### Exposing a Flow over HTTP

The `transport/http` subpackage runs an HTTP server alongside a `Flow`. It serves
two endpoints:

- `POST /credentials` — accepts `{"ssid": "...", "psk": "..."}` and forwards it
  to `Flow.Submit`. Returns `202 Accepted` on success, `400` for invalid
  credentials, `409` if a submission is already pending.
- `GET /events` — a Server-Sent Events stream of `FlowUpdate`s as they happen.

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
    p, err := iotflow.NewNetworkManagerProvisioner("wlan0")
    if err != nil {
        log.Fatal(err)
    }
    defer p.Close()

    f, err := iotflow.NewFlow(p)
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

If you'd rather mount the provisioning endpoints into your own server, use
`NewHandler(f, opts...)`, which returns an `http.Handler` and does not start a
listener of its own.

### Reading state updates

`Subscribe` returns a `<-chan FlowUpdate`. Multiple subscribers may observe
concurrently; each channel is buffered, and a subscriber that falls behind has
updates dropped rather than blocking the state machine. Every subscription is
closed when `Begin` returns — subscribing afterwards yields an already-closed
channel. Call `Unsubscribe` to release one early.

```go
type FlowUpdate struct {
    State FlowState // see below
    Err   error     // non-nil only when State == StateFailed
}
```

`FlowState` transitions through: `Idle` → `CheckingConnection` → `EnablingAP` →
`WaitingForCredentials` → `DisablingAP` → `Connecting`, ending in one of the
terminal states `Connected` (device was already online at `Begin`),
`Provisioned` (completed the full flow), or `Failed`. `FlowUpdate` marshals to
JSON as `{"state": "...", "error": "..."}` for transports that stream it.

## API

### Backend

```
NewNetworkManagerProvisioner(iface string) (*NetworkManagerProvisioner, error)

type Provisioner interface {
    IsConnected(ctx context.Context) (bool, error)
    EnableAPMode(ctx context.Context, ssid, psk string) (<-chan ProvisionUpdate, error)
    DisableAPMode() error
    ConnectToNetwork(ctx context.Context, ssid, psk string) (<-chan ProvisionUpdate, error)
    Close() error
}
```

`iface == ""` auto-discovers the first WiFi device; otherwise the named
interface is looked up and verified to be a WiFi adapter.

### Flow

```
NewFlow(p Provisioner, opts ...FlowOpt) (*Flow, error)

(*Flow).Begin(ctx context.Context) error       // runs the lifecycle; call once, blocks until terminal
(*Flow).Submit(ssid, psk string) error         // hand station credentials to the running Flow
(*Flow).Subscribe() <-chan FlowUpdate           // observe state transitions
(*Flow).Unsubscribe(ch <-chan FlowUpdate)       // release a subscription early
(*Flow).Finish() error                          // tear down (closes the backend)
```

### HTTP transport (`transport/http`)

```
Serve(ctx context.Context, f *iotflow.Flow, opts ...HandlerOpt) error
NewHandler(f Flow, opts ...HandlerOpt) http.Handler

WithAddress(addr string) HandlerOpt              // default ":80"
WithListener(ln net.Listener) HandlerOpt         // supply a pre-bound listener
WithErrorHandler(func(r *http.Request, err error)) HandlerOpt
```

### Errors

```
ErrSSIDInvalid       // SSID must be 1–32 characters
ErrPSKInvalid        // PSK must be 8–63 characters
ErrSubmissionPending // a previous Submit has not yet been consumed
```

**Credential constraints** (enforced by `Submit` and by the backend before any
network call):
- SSID: 1–32 characters
- PSK: 8–63 characters

> **Note:** AP credentials are configured via `FlowOpt`s passed to `NewFlow`.
> Until an option for them is wired up, the device's AP is brought up with empty
> credentials — pin your AP SSID/PSK once the corresponding `FlowOpt` lands.

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
it to `NewFlow`:

```go
p := myCustomBackend{}
f, err := iotflow.NewFlow(p)
```

The `Flow` type and all provisioning logic are backend-agnostic.
</content>
</invoke>
