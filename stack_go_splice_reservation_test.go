//go:build (linux && !android) || (darwin && !ios)

package tun

import (
	"bytes"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

type spliceReservationTestIO struct {
	*downloadSpliceTestIO
	entered chan struct{}
	resume  chan struct{}
}

func (p *spliceReservationTestIO) registerSocket(socket *goSocket, token uint32, interest uint8) error {
	close(p.entered)
	<-p.resume
	return p.downloadSpliceTestIO.registerSocket(socket, token, interest)
}

func TestGoFullSpliceReservationDuringEngage(t *testing.T) {
	for _, outcome := range []string{"active", "registration_failure", "close"} {
		t.Run(outcome, func(t *testing.T) {
			conn, platform := newDownloadSpliceTestConn(t)
			pause := &spliceReservationTestIO{downloadSpliceTestIO: platform, entered: make(chan struct{}), resume: make(chan struct{})}
			conn.engine.platformIO = pause
			if outcome == "registration_failure" {
				platform.registerErr = errors.New("registration failed")
			}
			fullSource, fullPeer := downloadSpliceSocketPair(t)
			foreign, _ := downloadSpliceSocketPair(t)
			completed := make(chan error, 2)
			if !conn.Splice(&kernelSocket{TCPConn: fullSource}, SpliceOptions{OnClose: func(err error) { completed <- err }}) {
				t.Fatal("full splice declined")
			}
			finished := make(chan struct{})
			go func() {
				defer close(finished)
				conn.engine.drainControlQueue()
			}()
			var once sync.Once
			resume := func() { once.Do(func() { close(pause.resume) }) }
			defer func() { resume(); <-finished }()
			select {
			case <-pause.entered:
			case <-time.After(time.Second):
				t.Fatal("full splice did not reach socket registration")
			}
			// The old pending pointer has been consumed, but ownership is not yet
			// active. Both APIs must still see a continuous full reservation.
			conn.access.Lock()
			reserved, pending := conn.spliced, conn.splicePending
			conn.access.Unlock()
			if !reserved || pending != nil {
				t.Fatal("pending-to-active full reservation was lost")
			}
			if conn.SpliceUpload(foreign, UploadSpliceOptions{OnWriteDone: func(err error) { completed <- err }}) ||
				conn.SpliceDownload(foreign, DownloadSpliceOptions{OnReadDone: func(err error) { completed <- err }}) ||
				conn.Splice(&kernelSocket{TCPConn: foreign}, SpliceOptions{}) {
				t.Fatal("another handoff entered the full pending-to-active window")
			}
			if _, err := foreign.Write([]byte{1}); err != nil {
				t.Fatal("declined handoff changed caller's socket: ", err)
			}
			if outcome == "close" {
				conn.Close()
			}
			resume()
			<-finished
			conn.engine.drainControlQueue()
			if outcome == "active" {
				if conn.splice == nil || conn.splice.directional || !conn.spliced {
					t.Fatal("full ownership did not become active")
				}
				payload := []byte("full splice retains the correct peer")
				conn.engine.deliverInOrder(conn, payload)
				conn.engine.flushSpliceDirty()
				actual := make([]byte, len(payload))
				if _, err := io.ReadFull(fullPeer, actual); err != nil || !bytes.Equal(actual, payload) {
					t.Fatalf("full splice peer changed: %v", err)
				}
				if len(completed) != 0 {
					t.Fatal("declined directional callback ran")
				}
				conn.engine.spliceDetach(conn, net.ErrClosed)
			}
			directionalSpliceDone(t, completed, true)
			if len(completed) != 0 || conn.splice != nil || conn.spliced {
				t.Fatal("failed/closed full splice retained its reservation or completed twice")
			}
		})
	}
}

func TestGoDirectionalSpliceRejectsFullOwnerState(t *testing.T) {
	conn, _ := newDownloadSpliceTestConn(t)
	fullSource, _ := downloadSpliceSocketPair(t)
	fullDone := make(chan error, 2)
	if !conn.Splice(&kernelSocket{TCPConn: fullSource}, SpliceOptions{OnClose: func(err error) { fullDone <- err }}) {
		t.Fatal("full splice declined")
	}
	conn.engine.drainControlQueue()
	foreign, _ := downloadSpliceSocketPair(t)
	duplicate, err := goDirectionalSocket(foreign)
	if err != nil {
		t.Fatal(err)
	}
	directionDone := make(chan error, 2)
	// Exercise the engine's fail-closed guard independently of API eligibility.
	conn.access.Lock()
	conn.directionalSplice = &goDirectionalSpliceState{
		original: foreign, uploadClaimed: true,
		uploadPending: &goDirectionalSpliceRequest{
			socket: duplicate, hasSocket: true, upload: true,
			done: func(err error) { directionDone <- err },
		},
	}
	conn.access.Unlock()
	conn.engine.handleDirectionalSpliceEngage(conn)
	directionalSpliceDone(t, directionDone, true)
	directionalSpliceDone(t, fullDone, true)
	if !conn.dead || conn.splice != nil || conn.spliced || len(directionDone) != 0 || len(fullDone) != 0 {
		t.Fatal("invalid mixed ownership survived or completed twice")
	}
	if _, err := foreign.Write([]byte{1}); err != nil {
		t.Fatal("mixed-state refusal closed caller's original socket: ", err)
	}
}
