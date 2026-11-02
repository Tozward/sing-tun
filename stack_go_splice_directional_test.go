//go:build (linux && !android) || (darwin && !ios)

package tun

import (
	"bytes"
	"errors"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	N "github.com/sagernet/sing/common/network"
	"golang.org/x/sys/unix"
)

func directionalSpliceWait(t *testing.T, condition func() bool, step func()) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !condition() && time.Now().Before(deadline) {
		step()
		runtime.Gosched()
	}
	if !condition() {
		t.Fatal("directional splice did not make progress")
	}
}

func directionalSpliceDone(t *testing.T, done <-chan error, wantError bool) {
	t.Helper()
	select {
	case err := <-done:
		if (err != nil) != wantError {
			t.Fatalf("direction completion error = %v, want error %v", err, wantError)
		}
	default:
		t.Fatal("direction did not finish")
	}
}

func TestGoDirectionalSpliceAttachOrders(t *testing.T) {
	for _, first := range []string{"upload", "download"} {
		for _, pending := range []bool{false, true} {
			t.Run(first+map[bool]string{false: "/active", true: "/pending"}[pending], func(t *testing.T) {
				conn, _ := newDownloadSpliceTestConn(t)
				source, peer := downloadSpliceSocketPair(t)
				upDone, downDone := make(chan error, 2), make(chan error, 2)
				var upCount, downCount atomic.Int64
				up := func() bool {
					return conn.SpliceUpload(source, UploadSpliceOptions{
						ReadCounters: []N.CountFunc{func(n int64) { upCount.Add(n) }},
						OnWriteDone:  func(err error) { upDone <- err },
					})
				}
				down := func() bool {
					return conn.SpliceDownload(source, DownloadSpliceOptions{
						WriteCounters: []N.CountFunc{func(n int64) { downCount.Add(n) }},
						OnReadDone:    func(err error) { downDone <- err },
					})
				}
				attachFirst, attachSecond := up, down
				if first == "download" {
					attachFirst, attachSecond = down, up
				}
				if !attachFirst() {
					t.Fatal("first directional handoff declined")
				}
				if !pending {
					conn.engine.drainControlQueue()
				}
				if !attachSecond() {
					t.Fatal("second directional handoff declined")
				}
				conn.engine.drainControlQueue()
				if conn.splice == nil || !conn.splice.uploadEnabled || !conn.splice.downloadEnabled {
					t.Fatal("directions did not merge")
				}
				if flags, err := unix.FcntlInt(uintptr(conn.splice.socket.fd), unix.F_GETFD, 0); err != nil || flags&unix.FD_CLOEXEC == 0 {
					t.Fatalf("engine duplicate lacks CLOEXEC: %d, %v", flags, err)
				}
				if conn.SpliceUpload(source, UploadSpliceOptions{}) || conn.SpliceDownload(source, DownloadSpliceOptions{}) {
					t.Fatal("a direction was handed off twice")
				}
				if _, err := conn.Read(make([]byte, 1)); !errors.Is(err, net.ErrClosed) {
					t.Fatalf("application reader retained upload ownership: %v", err)
				}
				if _, err := conn.Write([]byte{1}); !errors.Is(err, net.ErrClosed) {
					t.Fatalf("application writer retained download ownership: %v", err)
				}
				upload := bytes.Repeat([]byte("upload"), 4096)
				conn.engine.deliverInOrder(conn, upload)
				conn.engine.flushSpliceDirty()
				got := make([]byte, len(upload))
				if _, err := io.ReadFull(peer, got); err != nil || !bytes.Equal(got, upload) || upCount.Load() != int64(len(upload)) {
					t.Fatalf("upload bytes/counter: count=%d, err=%v", upCount.Load(), err)
				}
				downlink := bytes.Repeat([]byte("download"), 2048)
				if _, err := peer.Write(downlink); err != nil {
					t.Fatal(err)
				}
				peer.CloseWrite()
				directionalSpliceWait(t, func() bool { return len(downDone) > 0 }, func() { conn.engine.spliceReadPeer(conn) })
				directionalSpliceDone(t, downDone, false)
				stored := bytes.Join(conn.transmitStore.appendRuns(nil, 1, len(downlink)), nil)
				if !bytes.Equal(stored, downlink) || downCount.Load() != int64(len(downlink)) {
					t.Fatal("download storage/counter differs")
				}
				if conn.splice == nil || !conn.splice.uploadEnabled || len(upDone) != 0 {
					t.Fatal("download EOF ended the live upload")
				}
				conn.engine.acceptFin(conn)
				conn.engine.spliceAfterInput(conn)
				conn.engine.flushSpliceDirty()
				directionalSpliceDone(t, upDone, false)
				var one [1]byte
				if _, err := peer.Read(one[:]); !errors.Is(err, io.EOF) {
					t.Fatalf("upload EOF did not half-close target: %v", err)
				}
				if _, err := source.SyscallConn(); err != nil {
					t.Fatal("handoff closed caller's original socket: ", err)
				}
				conn.engine.spliceDetach(conn, net.ErrClosed)
				if len(upDone) != 0 || len(downDone) != 0 || conn.splice != nil || conn.spliced {
					t.Fatal("completion repeated or duplicate survived")
				}
			})
		}
	}
}

func TestGoDirectionalSpliceAttachAfterOtherEOF(t *testing.T) {
	for _, first := range []string{"upload", "download"} {
		t.Run(first, func(t *testing.T) {
			conn, _ := newDownloadSpliceTestConn(t)
			source, peer := downloadSpliceSocketPair(t)
			upDone, downDone := make(chan error, 2), make(chan error, 2)
			attachUpload := func() bool {
				return conn.SpliceUpload(source, UploadSpliceOptions{OnWriteDone: func(err error) { upDone <- err }})
			}
			attachDownload := func() bool {
				return conn.SpliceDownload(source, DownloadSpliceOptions{OnReadDone: func(err error) { downDone <- err }})
			}
			if first == "upload" {
				if !attachUpload() {
					t.Fatal("upload declined")
				}
				conn.engine.drainControlQueue()
				if conn.splice.pollRegistered {
					t.Fatal("idle upload registered a socket with no polling interest")
				}
				framed := []byte("download still belongs to the protocol reader")
				if _, err := peer.Write(framed); err != nil {
					t.Fatal(err)
				}
				conn.engine.handleStreamEvent(conn, goSocketEvent{readable: true})
				read := make([]byte, len(framed))
				if _, err := io.ReadFull(source, read); err != nil || !bytes.Equal(read, framed) {
					t.Fatalf("upload-only handoff consumed original download: %v", err)
				}
				// Upload-only handoff must leave ordinary downloads available.
				if _, err := conn.Write([]byte("ordinary download")); err != nil {
					t.Fatal("upload handoff blocked application writer: ", err)
				}
				conn.engine.acceptFin(conn)
				conn.engine.spliceAfterInput(conn)
				conn.engine.flushSpliceDirty()
				directionalSpliceDone(t, upDone, false)
				if !attachDownload() {
					t.Fatal("download after upload EOF declined")
				}
				conn.engine.drainControlQueue()
				peer.CloseWrite()
				directionalSpliceWait(t, func() bool { return len(downDone) > 0 }, func() { conn.engine.spliceReadPeer(conn) })
				directionalSpliceDone(t, downDone, false)
			} else {
				if !attachDownload() {
					t.Fatal("download declined")
				}
				conn.engine.drainControlQueue()
				peer.CloseWrite()
				directionalSpliceWait(t, func() bool { return len(downDone) > 0 }, func() { conn.engine.spliceReadPeer(conn) })
				directionalSpliceDone(t, downDone, false)
				if !attachUpload() {
					t.Fatal("upload after download EOF declined")
				}
				conn.engine.drainControlQueue()
				conn.engine.acceptFin(conn)
				conn.engine.spliceAfterInput(conn)
				conn.engine.flushSpliceDirty()
				directionalSpliceDone(t, upDone, false)
			}
			if conn.splice != nil || conn.spliced {
				t.Fatal("completed directions retained engine socket")
			}
		})
	}
}

func TestGoDirectionalSpliceRefusalAndReadQuiescence(t *testing.T) {
	conn, _ := newDownloadSpliceTestConn(t)
	source, _ := downloadSpliceSocketPair(t)
	readDone := make(chan error, 1)
	go func() {
		var one [1]byte
		_, err := io.ReadFull(conn, one[:])
		readDone <- err
	}()
	directionalSpliceWait(t, conn.readerParked.Load, func() {})
	called := false
	if conn.SpliceUpload(source, UploadSpliceOptions{OnWriteDone: func(error) { called = true }}) {
		t.Fatal("pending reader was stolen")
	}
	conn.engine.deliverInOrder(conn, []byte{1})
	if err := <-readDone; err != nil || called {
		t.Fatalf("declined upload disturbed reader/callback: %v", err)
	}
	if !conn.SpliceUpload(source, UploadSpliceOptions{}) {
		t.Fatal("quiescent upload declined")
	}
	other, _ := downloadSpliceSocketPair(t)
	if conn.SpliceDownload(other, DownloadSpliceOptions{}) || conn.Splice(&kernelSocket{TCPConn: source}, SpliceOptions{}) {
		t.Fatal("different socket or full-ownership splice merged")
	}
	conn.engine.drainControlQueue()
	if _, err := other.Write([]byte{1}); err != nil {
		t.Fatal("refusal closed a foreign socket: ", err)
	}

	udp, err := net.DialUDP("udp4", nil, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9})
	if err != nil {
		t.Fatal(err)
	}
	defer udp.Close()
	fresh, _ := newDownloadSpliceTestConn(t)
	if fresh.SpliceUpload(udp, UploadSpliceOptions{}) || fresh.SpliceDownload(udp, DownloadSpliceOptions{}) {
		t.Fatal("datagram socket accepted as stream")
	}
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if fresh.SpliceUpload(listener, UploadSpliceOptions{}) || fresh.SpliceDownload(listener, DownloadSpliceOptions{}) {
		t.Fatal("unconnected listener accepted as stream")
	}
	uncomparable := struct {
		syscall.Conn
		marker []byte
	}{Conn: source}
	if fresh.SpliceUpload(uncomparable, UploadSpliceOptions{}) || fresh.SpliceDownload(uncomparable, DownloadSpliceOptions{}) {
		t.Fatal("noncomparable socket identity accepted")
	}
}

func TestGoDirectionalSpliceCloseAndFailure(t *testing.T) {
	for _, name := range []string{"close_pending", "close_active", "register_failure", "stop_pending", "close_read", "close_write"} {
		t.Run(name, func(t *testing.T) {
			conn, platform := newDownloadSpliceTestConn(t)
			source, _ := downloadSpliceSocketPair(t)
			upDone, downDone := make(chan error, 2), make(chan error, 2)
			if !conn.SpliceUpload(source, UploadSpliceOptions{OnWriteDone: func(err error) { upDone <- err }}) ||
				!conn.SpliceDownload(source, DownloadSpliceOptions{OnReadDone: func(err error) { downDone <- err }}) {
				t.Fatal("directional handoff declined")
			}
			if name == "close_active" || name == "close_read" || name == "close_write" {
				conn.engine.drainControlQueue()
			}
			switch name {
			case "register_failure":
				platform.registerErr = errors.New("registration failed")
			case "stop_pending":
				conn.engine.engineState.Store(goEngineExited)
				conn.engine.releaseControlQueue()
			case "close_read":
				conn.CloseRead()
			case "close_write":
				conn.CloseWrite()
			default:
				conn.Close()
			}
			conn.engine.drainControlQueue()
			if name == "close_read" {
				directionalSpliceDone(t, upDone, true)
				if len(downDone) != 0 || conn.splice == nil || !conn.splice.downloadEnabled {
					t.Fatal("CloseRead cancelled independent download")
				}
				conn.engine.spliceDetach(conn, net.ErrClosed)
				directionalSpliceDone(t, downDone, true)
			} else if name == "close_write" {
				directionalSpliceDone(t, downDone, true)
				if len(upDone) != 0 || conn.splice == nil || !conn.splice.uploadEnabled {
					t.Fatal("CloseWrite cancelled independent upload")
				}
				conn.engine.spliceDetach(conn, net.ErrClosed)
				directionalSpliceDone(t, upDone, true)
			} else {
				directionalSpliceDone(t, upDone, true)
				directionalSpliceDone(t, downDone, true)
			}
			conn.engine.spliceDetach(conn, net.ErrClosed)
			if len(upDone) != 0 || len(downDone) != 0 || conn.splice != nil || conn.spliced {
				t.Fatal("cancelled handoff completed twice or retained socket")
			}
			if _, err := source.Write([]byte{1}); err != nil {
				t.Fatal("cancellation closed or shut down caller's socket: ", err)
			}
		})
	}
}

func TestGoDirectionalSpliceCloseRace(t *testing.T) {
	for range 64 {
		conn, _ := newDownloadSpliceTestConn(t)
		source, _ := downloadSpliceSocketPair(t)
		var acceptedUp, acceptedDown bool
		var completedUp, completedDown atomic.Int32
		var work sync.WaitGroup
		work.Add(3)
		go func() {
			defer work.Done()
			acceptedUp = conn.SpliceUpload(source, UploadSpliceOptions{OnWriteDone: func(error) { completedUp.Add(1) }})
		}()
		go func() {
			defer work.Done()
			acceptedDown = conn.SpliceDownload(source, DownloadSpliceOptions{OnReadDone: func(error) { completedDown.Add(1) }})
		}()
		go func() { defer work.Done(); conn.Close() }()
		work.Wait()
		conn.engine.drainControlQueue()
		want := func(accepted bool) int32 {
			if accepted {
				return 1
			}
			return 0
		}
		if completedUp.Load() != want(acceptedUp) || completedDown.Load() != want(acceptedDown) || conn.splice != nil || conn.spliced {
			t.Fatal("close race lost callback or retained directional state")
		}
	}
}

func TestGoDirectionalSpliceUploadBackpressure(t *testing.T) {
	conn, _ := newDownloadSpliceTestConn(t)
	source, peer := downloadSpliceSocketPair(t)
	if err := source.SetWriteBuffer(4096); err != nil {
		t.Fatal(err)
	}
	if err := peer.SetReadBuffer(1024); err != nil {
		t.Fatal(err)
	}
	var counted atomic.Int64
	done := make(chan error, 2)
	if !conn.SpliceUpload(source, UploadSpliceOptions{
		ReadCounters: []N.CountFunc{func(n int64) { counted.Add(n) }},
		OnWriteDone:  func(err error) { done <- err },
	}) {
		t.Fatal("upload declined")
	}
	conn.engine.drainControlQueue()
	payload := bytes.Repeat([]byte("queued-upload"), 16384)
	conn.engine.deliverInOrder(conn, payload)
	conn.engine.acceptFin(conn)
	conn.engine.spliceAfterInput(conn)
	conn.engine.flushSpliceDirty()
	if conn.splice == nil || !conn.splice.blocked || counted.Load() >= int64(len(payload)) || len(done) != 0 {
		t.Fatal("upload did not retain queued bytes under socket backpressure")
	}
	readDone := make(chan error, 1)
	go func() {
		got := make([]byte, len(payload))
		_, err := io.ReadFull(peer, got)
		if err == nil && !bytes.Equal(got, payload) {
			err = errors.New("backpressured upload bytes differ")
		}
		readDone <- err
	}()
	directionalSpliceWait(t, func() bool { return len(done) > 0 }, func() {
		conn.engine.handleStreamEvent(conn, goSocketEvent{writable: true})
	})
	directionalSpliceDone(t, done, false)
	if err := <-readDone; err != nil || counted.Load() != int64(len(payload)) {
		t.Fatalf("backpressured upload count=%d: %v", counted.Load(), err)
	}
}

func TestGoDirectionalSplicePendingUploadDrainsAtGracefulClose(t *testing.T) {
	conn, _ := newDownloadSpliceTestConn(t)
	source, peer := downloadSpliceSocketPair(t)
	payload := []byte("accepted upload must survive final client ACK")
	conn.engine.deliverInOrder(conn, payload)
	conn.engine.acceptFin(conn)
	done := make(chan error, 2)
	if !conn.SpliceUpload(source, UploadSpliceOptions{OnWriteDone: func(err error) { done <- err }}) {
		t.Fatal("upload declined")
	}
	// Model the final ACK closing native TCP before the queued handoff message
	// has run. Receive storage must be drained, as it is for an active full splice.
	conn.engine.detachConn(conn, nil, goDeathFinLinger)
	conn.engine.flushSpliceDirty()
	conn.engine.drainControlQueue()
	directionalSpliceDone(t, done, false)
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(peer, got); err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("graceful close dropped accepted upload: %v", err)
	}
	var one [1]byte
	if _, err := peer.Read(one[:]); !errors.Is(err, io.EOF) {
		t.Fatalf("drained upload did not half-close peer: %v", err)
	}
}

func TestGoDirectionalSpliceEngageRace(t *testing.T) {
	for range 64 {
		conn, _ := newDownloadSpliceTestConn(t)
		source, _ := downloadSpliceSocketPair(t)
		var completed atomic.Int32
		var acceptedUp, acceptedDown bool
		stop := make(chan struct{})
		var engineWork sync.WaitGroup
		engineWork.Add(1)
		go func() {
			defer engineWork.Done()
			for {
				conn.engine.drainControlQueue()
				select {
				case <-stop:
					conn.engine.drainControlQueue()
					return
				default:
					runtime.Gosched()
				}
			}
		}()
		var attach sync.WaitGroup
		attach.Add(2)
		go func() {
			defer attach.Done()
			acceptedUp = conn.SpliceUpload(source, UploadSpliceOptions{OnWriteDone: func(error) { completed.Add(1) }})
		}()
		go func() {
			defer attach.Done()
			acceptedDown = conn.SpliceDownload(source, DownloadSpliceOptions{OnReadDone: func(error) { completed.Add(1) }})
		}()
		attach.Wait()
		conn.Close()
		close(stop)
		engineWork.Wait()
		want := int32(0)
		if acceptedUp {
			want++
		}
		if acceptedDown {
			want++
		}
		if completed.Load() != want || conn.splice != nil || conn.spliced {
			t.Fatal("pending-to-active race lost callback or retained socket")
		}
	}
}

func TestGoKernelDirectionalSplice(t *testing.T) {
	fixture := newKernelStackFixture(t, kernelStackConfig{mtu: 1500})
	for _, ipv6 := range []bool{false, true} {
		for _, uploadFirst := range []bool{false, true} {
			client, conn := fixture.pair(t, ipv6)
			source, peer := downloadSpliceSocketPair(t)
			upDone, downDone := make(chan error, 1), make(chan error, 1)
			var upCount, downCount atomic.Int64
			up := func() bool {
				return conn.SpliceUpload(source, UploadSpliceOptions{
					ReadCounters: []N.CountFunc{func(n int64) { upCount.Add(n) }},
					OnWriteDone:  func(err error) { upDone <- err },
				})
			}
			down := func() bool {
				return conn.SpliceDownload(source, DownloadSpliceOptions{
					WriteCounters: []N.CountFunc{func(n int64) { downCount.Add(n) }},
					OnReadDone:    func(err error) { downDone <- err },
				})
			}
			first, second := down, up
			if uploadFirst {
				first, second = up, down
			}
			if !first() || !second() {
				t.Fatal("kernel directional handoff declined")
			}
			upload, download := kernelPayload(32768, 61), kernelPayload(32768, 113)
			if _, err := client.Write(upload); err != nil {
				t.Fatal(err)
			}
			got := make([]byte, len(upload))
			if _, err := io.ReadFull(peer, got); err != nil || !bytes.Equal(got, upload) {
				t.Fatalf("kernel upload: %v", err)
			}
			if _, err := peer.Write(download); err != nil {
				t.Fatal(err)
			}
			peer.CloseWrite()
			got, err := io.ReadAll(client)
			if err != nil || !bytes.Equal(got, download) {
				t.Fatalf("kernel download through EOF: %v", err)
			}
			select {
			case err := <-downDone:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("kernel download callback waited for upload EOF")
			}
			if _, err := client.Write(upload); err != nil {
				t.Fatal("kernel upload stopped at download EOF: ", err)
			}
			got = make([]byte, len(upload))
			if _, err := io.ReadFull(peer, got); err != nil || !bytes.Equal(got, upload) {
				t.Fatalf("kernel upload after download EOF: %v", err)
			}
			client.CloseWrite()
			var one [1]byte
			if _, err := peer.Read(one[:]); !errors.Is(err, io.EOF) {
				t.Fatalf("kernel upload EOF did not half-close target: %v", err)
			}
			select {
			case err := <-upDone:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("kernel upload callback did not finish")
			}
			if upCount.Load() != int64(2*len(upload)) || downCount.Load() != int64(len(download)) {
				t.Fatalf("kernel counters: upload=%d, download=%d", upCount.Load(), downCount.Load())
			}
		}
	}
}
