//go:build linux && !android

package tun

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestNativeTunOwnedQueuesCloseOnExec(t *testing.T) {
	device, err := New(Options{Name: fmt.Sprintf("xclo%d", os.Getpid()), MTU: 1500, MultiQueue: true, EXP_ExternalConfiguration: true})
	if err != nil {
		if errors.Is(err, unix.EPERM) || errors.Is(err, unix.EACCES) || errors.Is(err, unix.ENOENT) {
			t.Skipf("requires a usable /dev/net/tun and CAP_NET_ADMIN: %v", err)
		}
		t.Fatal(err)
	}
	defer device.Close()
	nativeTun := device.(*NativeTun)
	check := func(fd int) {
		t.Helper()
		flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
		if err != nil || flags&unix.FD_CLOEXEC == 0 {
			t.Fatalf("TUN descriptor %d is not close-on-exec: flags=%d, err=%v", fd, flags, err)
		}
	}
	check(nativeTun.tunFd)
	if !nativeTun.multiQueue {
		t.Skip("kernel does not support multi-queue TUN")
	}
	queueFD, err := nativeTun.openQueue()
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(queueFD)
	check(queueFD)
}

func TestNativeTunOwnedConfigureFailureDoesNotCloseReusedDescriptor(t *testing.T) {
	// The TUN takes the lowest free descriptor. Reuse that slot immediately after
	// the invalid MTU forces New to fail, then let the abandoned os.File be collected.
	slot, err := unix.Open("/dev/null", unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	unix.Close(slot)
	device, err := New(Options{Name: fmt.Sprintf("xfail%d", os.Getpid()), MTU: 1, EXP_ExternalConfiguration: true})
	if device != nil {
		device.Close()
		t.Skip("kernel accepted the test MTU or netlink configuration is not permitted")
	}
	if errors.Is(err, unix.EPERM) || errors.Is(err, unix.EACCES) || errors.Is(err, unix.ENOENT) {
		t.Skipf("requires a usable /dev/net/tun and CAP_NET_ADMIN: %v", err)
	}
	if err == nil || !strings.Contains(err.Error(), "set mtu") {
		t.Fatalf("expected MTU configuration failure, got %v", err)
	}
	reused, err := unix.Open("/dev/null", unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(reused)
	if reused != slot {
		t.Skipf("descriptor slot changed concurrently: got %d, want %d", reused, slot)
	}
	deadline := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(deadline) {
		runtime.GC()
		runtime.Gosched()
		if _, err = unix.FcntlInt(uintptr(reused), unix.F_GETFD, 0); err != nil {
			t.Fatalf("abandoned TUN owner closed the reused descriptor: %v", err)
		}
	}
}
