// Package nmfake provides a scriptable, in-process fake of the NetworkManager
// D-Bus service for integration-testing the iotflow NetworkManagerProvisioner
// end-to-end.
//
// The fake owns the real bus name (org.freedesktop.NetworkManager) on a private
// D-Bus daemon that the test starts. Because godbus's dbus.SystemBus() honours
// the DBUS_SYSTEM_BUS_ADDRESS environment variable, pointing that variable at
// the private bus makes the *unmodified* production provisioner talk to this
// fake as though it were the system NetworkManager — no test-only seam in the
// provisioner is required.
//
// This is a regular (non-_test) source file so it can be imported by tests in
// any package of the module; it lives under internal/ to stay out of the public
// API. Bus and fake construction return errors (rather than taking a
// *testing.T) so they can run from TestMain, which has no T available.
package nmfake

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// busConfigTemplate is a minimal session-style bus configuration listening on a
// private unix socket. EXTERNAL auth is all godbus needs for a same-user socket.
const busConfigTemplate = `<!DOCTYPE busconfig PUBLIC "-//freedesktop//DTD D-Bus Bus Configuration 1.0//EN" "http://www.freedesktop.org/standards/dbus/1.0/busconfig.dtd">
<busconfig>
  <type>session</type>
  <listen>unix:path=%s</listen>
  <auth>EXTERNAL</auth>
  <policy context="default">
    <allow send_destination="*" eavesdrop="true"/>
    <allow eavesdrop="true"/>
    <allow own="*"/>
  </policy>
</busconfig>
`

// StartBus launches an isolated dbus-daemon on a private unix socket and returns
// its bus address. The returned cleanup stops the daemon and removes its temp
// directory; callers must invoke it (e.g. from TestMain) when finished.
func StartBus() (addr string, cleanup func(), err error) {
	dir, err := os.MkdirTemp("", "nmfake-bus-*")
	if err != nil {
		return "", nil, fmt.Errorf("creating temp dir: %w", err)
	}
	removeDir := func() { _ = os.RemoveAll(dir) }

	sock := filepath.Join(dir, "bus")
	cfgPath := filepath.Join(dir, "bus.conf")
	if err := os.WriteFile(cfgPath, []byte(fmt.Sprintf(busConfigTemplate, sock)), 0o600); err != nil {
		removeDir()
		return "", nil, fmt.Errorf("writing bus config: %w", err)
	}

	cmd := exec.Command("dbus-daemon", "--config-file="+cfgPath, "--print-address", "--nofork")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		removeDir()
		return "", nil, fmt.Errorf("dbus-daemon stdout pipe: %w", err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		removeDir()
		return "", nil, fmt.Errorf("starting dbus-daemon (is it installed?): %w", err)
	}

	stop := func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		removeDir()
	}

	// dbus-daemon prints its full address (with guid) on the first line of stdout.
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		stop()
		return "", nil, fmt.Errorf("reading dbus-daemon address: %w", err)
	}
	addr = strings.TrimSpace(line)
	if addr == "" {
		stop()
		return "", nil, fmt.Errorf("dbus-daemon returned an empty address")
	}

	return addr, stop, nil
}
