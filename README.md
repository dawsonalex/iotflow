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

## Usage

### Basic provisioning flow

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/dawsonalex/iotflow"
)

func main() {
    // Auto-discovers the first available WiFi device.
    p, err := iotflow.NewProvisioner()
    if err != nil {
        log.Fatal(err)
    }
    defer p.Close()

    ctx := context.Background()

    // Skip provisioning if already connected.
    connected, err := p.IsConnected(ctx)
    if err != nil {
        log.Fatal(err)
    }
    if connected {
        fmt.Println("already connected")
        return
    }

    // Enter AP mode so the user can connect and supply credentials.
    apUpdates, err := p.EnableAPMode(ctx, "MyDevice-Setup", "setuppassword")
    if err != nil {
        log.Fatal(err)
    }
    for upd := range apUpdates {
        if upd.Err != nil {
            log.Fatal(upd.Err)
        }
        fmt.Printf("AP: %s\n", upd.State)
    }

    // --- At this point, receive SSID + password from the user via your
    // provisioning server, then disable AP mode and connect. ---

    ssid, psk := receiveCredentials()

    if err := p.DisableAPMode(); err != nil {
        log.Fatal(err)
    }

    // Connect to the target network.
    updates, err := p.ConnectToNetwork(ctx, ssid, psk)
    if err != nil {
        log.Fatal(err)
    }
    for upd := range updates {
        if upd.Err != nil {
            log.Fatalf("connection failed: %v", upd.Err)
        }
        fmt.Printf("station: %s\n", upd.State)
    }

    fmt.Println("connected")
}
```

### Targeting a specific interface

If your device has multiple WiFi adapters, or you want to pin to a known interface name:

```go
p, err := iotflow.NewProvisioner(iotflow.WithInterface("wlan0"))
```

### Reading state updates

`EnableAPMode` and `ConnectToNetwork` both return a `<-chan ProvisionUpdate` that is closed when a terminal state is reached or the context is cancelled.

```go
type ProvisionUpdate struct {
    State ProvisionState // Connecting, Connected, or Failed
    Err   error          // non-nil only on Failed
}
```

A `Failed` update always carries a non-nil `Err`. `Connected` signals success and closes the channel immediately after. Cancelling the context closes the channel with no final update — it is the caller's responsibility to decide what to do next.

## API

```
NewProvisioner(opts ...Option) (*Provisioner, error)
WithInterface(iface string) Option

(*Provisioner).IsConnected(ctx context.Context) (bool, error)
(*Provisioner).EnableAPMode(ctx context.Context, ssid, psk string) (<-chan ProvisionUpdate, error)
(*Provisioner).DisableAPMode() error
(*Provisioner).ConnectToNetwork(ctx context.Context, ssid, psk string) (<-chan ProvisionUpdate, error)
(*Provisioner).Close() error
```

**Credential constraints** (enforced before any network call):
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

The NetworkManager implementation is one backend. To support a different system (e.g. `wpa_supplicant`), implement the unexported `backend` interface and wire it up with a new constructor. The `Provisioner` type and all provisioning logic are backend-agnostic.
