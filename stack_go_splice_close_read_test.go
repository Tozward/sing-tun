//go:build (linux && !android) || (darwin && !ios)

package tun

import (
	"bytes"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"

	N "github.com/sagernet/sing/common/network"
	"golang.org/x/sys/unix"
)

func TestGoDirectionalSpliceCloseReadCancelsFinLingerUpload(t *testing.T) {
	for _, test := range []struct {
		name           string
		pending        bool
		closeBeforeACK bool
		concurrentACK  bool
	}{
		{name: "active_before_final_ack", closeBeforeACK: true},
		{name: "active_after_final_ack"},
		{name: "active_concurrent_final_ack", concurrentACK: true},
		{name: "pending_after_final_ack", pending: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			conn, _ := newDownloadSpliceTestConn(t)
			source, peer := directionalSpliceUnixSocketPair(t)
			if err := source.SetWriteBuffer(4096); err != nil {
				t.Fatal(err)
			}
			var counted atomic.Int64
			upDone := make(chan error, 2)
			attachUpload := func() {
				if !conn.SpliceUpload(source, UploadSpliceOptions{
					ReadCounters: []N.CountFunc{func(n int64) { counted.Add(n) }},
					OnWriteDone:  func(err error) { upDone <- err },
				}) {
					t.Fatal("upload handoff declined")
				}
			}
			if test.pending {
				// The ordinary download writer sends its FIN before the upload
				// handoff is accepted. Its pending request must survive the ACK.
				if err := conn.CloseWrite(); err != nil {
					t.Fatal(err)
				}
				conn.engine.drainControlQueue()
			} else {
				downDone := make(chan error, 2)
				if !conn.SpliceDownload(source, DownloadSpliceOptions{OnReadDone: func(err error) { downDone <- err }}) {
					t.Fatal("download handoff declined")
				}
				conn.engine.drainControlQueue()
				if err := peer.CloseWrite(); err != nil {
					t.Fatal(err)
				}
				conn.engine.spliceReadPeer(conn)
				directionalSpliceDone(t, downDone, false)
				attachUpload()
				conn.engine.drainControlQueue()
			}
			if !conn.finSent {
				t.Fatal("native download FIN was not sent")
			}
			payload := bytes.Repeat([]byte("retained-upload"), 16384)
			conn.engine.deliverInOrder(conn, payload)
			conn.engine.acceptFin(conn)
			if test.pending {
				attachUpload()
			} else {
				conn.engine.spliceAfterInput(conn)
				conn.engine.flushSpliceDirty()
			}
			if test.closeBeforeACK {
				// Queue CloseRead, then process the final ACK before its control
				// message. This ordering must also release retained receive data.
				if err := conn.CloseRead(); err != nil {
					t.Fatal(err)
				}
			}
			var readDone chan error
			if test.concurrentACK {
				readDone = make(chan error, 1)
				go func() { readDone <- conn.CloseRead() }()
			}
			conn.finAcked = true
			conn.engine.advanceFinState(conn)
			if readDone != nil {
				if err := <-readDone; err != nil {
					t.Fatal(err)
				}
			}
			if !conn.dead || !conn.drainable || conn.splice == nil || !conn.splice.uploadEnabled || !conn.splice.blocked ||
				counted.Load() <= 0 || counted.Load() >= int64(len(payload)) || len(upDone) != 0 {
				t.Fatal("final native ACK did not retain the backpressured upload")
			}
			duplicate := conn.splice.socket.fd
			if !test.closeBeforeACK && !test.concurrentACK {
				// There is no symmetric download left after native TCP finishes:
				// CloseWrite must remain a no-op and leave retained upload alone.
				if err := conn.CloseWrite(); err != nil {
					t.Fatal(err)
				}
				conn.engine.drainControlQueue()
				if conn.readShut || conn.splice == nil || !conn.splice.uploadEnabled || len(upDone) != 0 {
					t.Fatal("CloseWrite cancelled the retained upload")
				}
				if err := conn.CloseRead(); err != nil {
					t.Fatal(err)
				}
			}
			conn.engine.drainControlQueue()
			select {
			case err := <-upDone:
				if !errors.Is(err, net.ErrClosed) {
					t.Fatalf("cancelled upload callback: %v", err)
				}
			default:
				t.Fatal("CloseRead left the FIN-linger upload active")
			}
			if conn.splice != nil || conn.spliced || conn.drainable || conn.engine.spliceDirtyList != nil {
				t.Fatal("cancelled upload retained socket, receive ownership or dirty-list state")
			}
			if _, err := unix.FcntlInt(uintptr(duplicate), unix.F_GETFD, 0); !errors.Is(err, unix.EBADF) {
				t.Fatalf("cancelled upload duplicate remains open: %v", err)
			}
			conn.engine.reapDying()
			if !conn.released || conn.slabHolder.heldBytes() != 0 {
				t.Fatal("cancelled FIN-linger upload retained receive storage")
			}
			prefix := make([]byte, int(counted.Load()))
			if _, err := io.ReadFull(peer, prefix); err != nil || !bytes.Equal(prefix, payload[:len(prefix)]) {
				t.Fatalf("partial upload bytes: %v", err)
			}
			// Cancelling native upload releases only its duplicate; the caller's
			// socket remains writable and must not have received SHUT_WR.
			marker := []byte("caller still owns the socket")
			if _, err := source.Write(marker); err != nil {
				t.Fatal("CloseRead closed or half-closed caller socket: ", err)
			}
			got := make([]byte, len(marker))
			if _, err := io.ReadFull(peer, got); err != nil || !bytes.Equal(got, marker) {
				t.Fatalf("caller socket after cancellation: %v", err)
			}
			if err := conn.CloseRead(); err != nil {
				t.Fatal(err)
			}
			conn.engine.drainControlQueue()
			conn.engine.flushSpliceDirty()
			if len(upDone) != 0 {
				t.Fatal("CloseRead completed the upload twice")
			}
		})
	}
}

func TestGoDirectionalSpliceCloseReadPreservesOrdinaryDeadDrain(t *testing.T) {
	conn, _ := newDownloadSpliceTestConn(t)
	payload := []byte("ordinary dead connection keeps its buffered read semantics")
	conn.engine.deliverInOrder(conn, payload)
	conn.engine.detachConn(conn, nil, goDeathFinLinger)
	if err := conn.CloseRead(); err != nil {
		t.Fatal(err)
	}
	conn.engine.drainControlQueue()
	if conn.readShut || !conn.drainable {
		t.Fatal("CloseRead changed an ordinary already-dead connection")
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(conn, got); err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("ordinary buffered read changed: %v", err)
	}
}
