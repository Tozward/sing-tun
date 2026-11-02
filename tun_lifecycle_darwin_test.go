//go:build darwin && !ios

package tun

import (
	"errors"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func TestNativeTunExternalDescriptorOwnership(t *testing.T) {
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_DGRAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fds[1])
	device, err := New(Options{FileDescriptor: fds[0], MTU: 1500, EXP_ExternalConfiguration: true})
	if err != nil {
		unix.Close(fds[0])
		t.Fatal(err)
	}
	nativeTun := device.(*NativeTun)
	for _, fd := range []int{nativeTun.stopFd.ReadFD, nativeTun.stopFd.WriteFD} {
		flags, flagErr := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
		if flagErr != nil || flags&unix.FD_CLOEXEC == 0 {
			device.Close()
			t.Fatalf("stop pipe descriptor %d is not close-on-exec: flags=%d, err=%v", fd, flags, flagErr)
		}
	}
	if err = device.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = unix.FcntlInt(uintptr(fds[0]), unix.F_GETFD, 0); !errors.Is(err, unix.EBADF) {
		t.Fatalf("successful New did not transfer descriptor ownership: %v", err)
	}
	if _, err = unix.FcntlInt(uintptr(fds[1]), unix.F_GETFD, 0); err != nil {
		t.Fatalf("closing TUN closed an unrelated descriptor: %v", err)
	}
}

func TestNativeTunConstructionFailureRetainsDescriptor(t *testing.T) {
	const stageEnvironment = "SING_TUN_CONSTRUCTION_FAILURE_STAGE"
	stage := os.Getenv(stageEnvironment)
	if stage == "" {
		for _, stage = range []string{"stop pipe", "read poller"} {
			t.Run(stage, func(t *testing.T) {
				command := exec.Command(os.Args[0], "-test.run=^TestNativeTunConstructionFailureRetainsDescriptor$")
				command.Env = append(os.Environ(), stageEnvironment+"="+stage)
				if output, err := command.CombinedOutput(); err != nil {
					t.Fatalf("constructor failure subprocess: %v\n%s", err, output)
				}
			})
		}
		return
	}
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_DGRAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fds[0])
	defer unix.Close(fds[1])
	// Initialize Go's process-wide poller before exhausting descriptors so the
	// failure is in the constructor's stop pipe or read poller, rather than the runtime.
	warmFD, err := unix.Dup(fds[1])
	if err != nil {
		t.Fatal(err)
	}
	if err = unix.SetNonblock(warmFD, true); err != nil {
		unix.Close(warmFD)
		t.Fatal(err)
	}
	if err = os.NewFile(uintptr(warmFD), "poller-warmup").Close(); err != nil {
		t.Fatal(err)
	}
	var limit unix.Rlimit
	if err = unix.Getrlimit(unix.RLIMIT_NOFILE, &limit); err != nil {
		t.Fatal(err)
	}
	reduced := limit
	reduced.Cur = min(reduced.Cur, 128)
	if err = unix.Setrlimit(unix.RLIMIT_NOFILE, &reduced); err != nil {
		t.Fatal(err)
	}
	var occupied []int
	for {
		fd, openErr := unix.Open("/dev/null", unix.O_RDONLY, 0)
		if errors.Is(openErr, unix.EMFILE) {
			break
		}
		if openErr != nil {
			t.Fatal(openErr)
		}
		occupied = append(occupied, fd)
	}
	available := 0
	if stage == "read poller" {
		available = 2
	}
	for range available {
		unix.Close(occupied[len(occupied)-1])
		occupied = occupied[:len(occupied)-1]
	}
	device, constructionErr := New(Options{FileDescriptor: fds[0], MTU: 1500, EXP_ExternalConfiguration: true})
	if device != nil {
		device.Close()
	}
	// Every slot released for the failing constructor must be available again.
	var returned []int
	for {
		fd, openErr := unix.Open("/dev/null", unix.O_RDONLY, 0)
		if openErr != nil {
			break
		}
		returned = append(returned, fd)
	}
	for _, fd := range append(returned, occupied...) {
		unix.Close(fd)
	}
	if err = unix.Setrlimit(unix.RLIMIT_NOFILE, &limit); err != nil {
		t.Fatal(err)
	}
	if device != nil || !errors.Is(constructionErr, unix.EMFILE) || !strings.Contains(constructionErr.Error(), "create "+stage) {
		t.Fatalf("expected %s allocation error, got device=%v, err=%v", stage, device, constructionErr)
	}
	if len(returned) != available {
		t.Fatalf("constructor leaked descriptors: got %d free slots, want %d", len(returned), available)
	}
	runtime.GC()
	if _, err = unix.FcntlInt(uintptr(fds[0]), unix.F_GETFD, 0); err != nil {
		t.Fatalf("failed constructor closed the supplied descriptor: %v", err)
	}
}

func TestUseSocketCloseOnExecAndFailureCleanup(t *testing.T) {
	wantErr := errors.New("socket callback failed")
	var descriptor int
	err := useSocket(unix.AF_INET, unix.SOCK_DGRAM, 0, func(fd int) error {
		descriptor = fd
		flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
		if err != nil || flags&unix.FD_CLOEXEC == 0 {
			t.Errorf("socket is not close-on-exec: flags=%d, err=%v", flags, err)
		}
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("callback error was lost: %v", err)
	}
	if _, err = unix.FcntlInt(uintptr(descriptor), unix.F_GETFD, 0); !errors.Is(err, unix.EBADF) {
		t.Fatalf("failed socket callback leaked its descriptor: %v", err)
	}
}
