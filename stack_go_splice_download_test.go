//go:build (linux && !android) || (darwin && !ios)

package tun

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"golang.org/x/sys/unix"
)

// Exercise real duplicated sockets without creating a privileged TUN. Packet
// delivery and acknowledgements are controlled by each test's engine calls.
type downloadSpliceTestIO struct {
	goPlatformIO
	registerErr error
	interest    uint8
}

func (*downloadSpliceTestIO) supportsSockets() bool { return true }
func (p *downloadSpliceTestIO) registerSocket(socket *goSocket, token uint32, interest uint8) error {
	if p.registerErr != nil {
		return p.registerErr
	}
	socket.token = token
	socket.registered = true
	p.interest = interest
	return nil
}
func (p *downloadSpliceTestIO) updateSocket(_ *goSocket, interest uint8) error {
	p.interest = interest
	return nil
}
func (p *downloadSpliceTestIO) unregisterSocket(socket *goSocket) {
	socket.registered = false
	p.interest = 0
}
func (*downloadSpliceTestIO) writeFrame([][]byte, ForwardFrameMeta) error { return nil }
func (*downloadSpliceTestIO) writePacket([]byte, ForwardFrameMeta) error  { return nil }
func (*downloadSpliceTestIO) writeData([][]byte, ForwardFrameMeta, *GoConn, uint64) error {
	return nil
}
func (*downloadSpliceTestIO) transmitBacklogBelowBatch(*GoConn) bool { return true }

func newDownloadSpliceTestConn(t *testing.T) (*GoConn, *downloadSpliceTestIO) {
	t.Helper()
	device := NewMemoryTun(MemoryTunOptions{MTU: 1500})
	stack, err := NewGo(StackOptions{
		Context: context.Background(), Tun: device, TunOptions: Options{MTU: 1500},
		Logger: logger.NOP(), TCPCongestionControl: "reno",
	})
	if err != nil {
		t.Fatal(err)
	}
	platform := &downloadSpliceTestIO{goPlatformIO: &goMemoryIO{stack: stack, tun: device}}
	engine := newGoEngine(stack, platform, 1)
	conn := new(GoConn)
	conn.initialize(engine, flowKey{}, M.ParseSocksaddr("10.0.0.2:12000"), M.ParseSocksaddr("203.0.113.1:443"))
	conn.ipVersion = 4
	conn.effectiveMSS.Store(1460)
	engine.initializeSession(conn, engine.coarseTime.Load(), 65535)
	conn.phase = goPhaseEstablished
	conn.state = goTCPEstablished
	close(conn.establishedSignal)
	t.Cleanup(func() {
		engine.spliceDetach(conn, net.ErrClosed)
		// Rejection cases temporarily lower the synthetic handshake phase.
		conn.phase = goPhaseEstablished
		engine.detachConn(conn, net.ErrClosed, goDeathImmediate)
		engine.engineState.Store(goEngineExited)
		engine.releaseControlQueue()
		engine.reapDying()
		device.Close()
	})
	return conn, platform
}

func downloadSpliceSocketPair(t *testing.T) (*net.TCPConn, *net.TCPConn) {
	t.Helper()
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	source, err := net.DialTCP("tcp4", nil, listener.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { source.Close() })
	peer, err := listener.AcceptTCP()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { peer.Close() })
	source.SetDeadline(time.Now().Add(3 * time.Second))
	peer.SetDeadline(time.Now().Add(3 * time.Second))
	return source, peer
}

func TestGoDownloadSpliceDirectionAndEOF(t *testing.T) {
	conn, _ := newDownloadSpliceTestConn(t)
	source, peer := downloadSpliceSocketPair(t)
	finished := make(chan error, 2)
	var counted atomic.Int64
	if !conn.SpliceDownload(source, DownloadSpliceOptions{
		WriteCounters: []N.CountFunc{func(n int64) { counted.Add(n) }},
		OnReadDone:    func(err error) { finished <- err },
	}) {
		t.Fatal("download splice refused")
	}
	conn.engine.drainControlQueue()
	if conn.splice == nil || !conn.splice.downloadOnly {
		t.Fatal("download splice did not engage")
	}
	duplicate := conn.splice.socket.fd
	if conn.SpliceDownload(source, DownloadSpliceOptions{}) {
		t.Fatal("second download splice accepted")
	}
	if _, err := conn.Write([]byte("must not race the native writer")); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("application write after handoff: %v", err)
	}

	// A parked normal reader still receives the client's upload directly.
	upload := []byte("client upload")
	readDone := make(chan error, 1)
	go func() {
		data := make([]byte, len(upload))
		_, err := io.ReadFull(conn, data)
		if err == nil && !bytes.Equal(data, upload) {
			err = errors.New("normal GoConn reader lost upload payload")
		}
		readDone <- err
	}()
	deadline := time.Now().Add(time.Second)
	for !conn.readerParked.Load() && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if !conn.readerParked.Load() {
		t.Fatal("normal reader did not park")
	}
	conn.engine.deliverInOrder(conn, upload)
	if err := <-readDone; err != nil {
		t.Fatal(err)
	}
	if _, err := source.Write(upload); err != nil {
		t.Fatal("original source socket stopped accepting upload: ", err)
	}
	data := make([]byte, len(upload))
	if _, err := io.ReadFull(peer, data); err != nil || !bytes.Equal(data, upload) {
		t.Fatalf("original source upload: %v", err)
	}

	downlink := []byte("native download")
	if _, err := peer.Write(downlink); err != nil {
		t.Fatal(err)
	}
	if err := peer.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	for counted.Load() != int64(len(downlink)) && time.Now().Before(deadline) {
		conn.engine.spliceReadPeer(conn)
		runtime.Gosched()
	}
	if counted.Load() != int64(len(downlink)) {
		t.Fatalf("downlink counter = %d", counted.Load())
	}
	stored := bytes.Join(conn.transmitStore.appendRuns(nil, 1, len(downlink)), nil)
	if !bytes.Equal(stored, downlink) {
		t.Fatal("native send storage changed downstream bytes")
	}
	for conn.splice != nil && time.Now().Before(deadline) {
		conn.engine.spliceReadPeer(conn)
		runtime.Gosched()
	}
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatal("read EOF did not complete while upload remained open")
	}
	if conn.splice != nil || !conn.writeShut || !conn.finPending || !conn.finSent {
		t.Fatal("EOF did not detach the reader and preserve a closed write half with FIN")
	}
	if _, err := unix.FcntlInt(uintptr(duplicate), unix.F_GETFD, 0); !errors.Is(err, unix.EBADF) {
		t.Fatalf("duplicated source descriptor survived EOF: %v", err)
	}
	if conn.SpliceDownload(source, DownloadSpliceOptions{}) || conn.Splice(&kernelSocket{TCPConn: source}, SpliceOptions{}) {
		t.Fatal("a permanently closed write half accepted a second handoff")
	}
	if _, err := source.Write(upload); err != nil {
		t.Fatal("EOF closed original upload socket: ", err)
	}
	if _, err := io.ReadFull(peer, data); err != nil || !bytes.Equal(data, upload) {
		t.Fatalf("upload after download EOF: %v", err)
	}
	conn.engine.spliceDetach(conn, net.ErrClosed)
	select {
	case err := <-finished:
		t.Fatalf("read completion was reported twice: %v", err)
	default:
	}
}

func TestGoDownloadSpliceBackpressure(t *testing.T) {
	conn, platform := newDownloadSpliceTestConn(t)
	conn.transmitCapacity = 32
	source, peer := downloadSpliceSocketPair(t)
	var counted int64
	if !conn.SpliceDownload(source, DownloadSpliceOptions{WriteCounters: []N.CountFunc{func(n int64) { counted += n }}}) {
		t.Fatal("download splice refused")
	}
	conn.engine.drainControlQueue()
	if _, err := peer.Write(bytes.Repeat([]byte{0x53}, 64)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for counted == 0 && time.Now().Before(deadline) {
		conn.engine.spliceReadPeer(conn)
		runtime.Gosched()
	}
	if counted != 32 || conn.bufferedTail.Load() != 33 || !conn.splice.readStalled || platform.interest&goInterestRead != 0 {
		t.Fatalf("downstream storage did not stop socket reads at its capacity: count=%d buffered=%d", counted, conn.bufferedTail.Load())
	}
	conn.sendUnacked.Store(33)
	conn.releaseTransmitted(33)
	conn.engine.spliceResume(conn)
	if conn.splice.readStalled || platform.interest&goInterestRead == 0 {
		t.Fatal("acknowledging buffered data did not re-enable source reads")
	}
	conn.engine.spliceReadPeer(conn)
	if counted != 64 || conn.bufferedTail.Load() != 65 {
		t.Fatalf("resumed downstream payload: count=%d buffered=%d", counted, conn.bufferedTail.Load())
	}
}

func TestGoDownloadSpliceZeroWindow(t *testing.T) {
	conn, platform := newDownloadSpliceTestConn(t)
	conn.transmitCapacity = 4 * goTransmitReadAhead
	conn.sendPermit.Store(goPermitWindowBit | 1)
	source, peer := downloadSpliceSocketPair(t)
	var counted int64
	if !conn.SpliceDownload(source, DownloadSpliceOptions{WriteCounters: []N.CountFunc{func(n int64) { counted += n }}}) {
		t.Fatal("download splice refused")
	}
	conn.engine.drainControlQueue()
	payload := bytes.Repeat([]byte{0x75}, goTransmitReadAhead+32)
	if _, err := peer.Write(payload); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for counted < goTransmitReadAhead && time.Now().Before(deadline) {
		conn.engine.spliceReadPeer(conn)
		runtime.Gosched()
	}
	if counted != goTransmitReadAhead || conn.sentTail.Load() != 1 || !conn.splice.readStalled || platform.interest&goInterestRead != 0 {
		t.Fatalf("zero window did not bound native read-ahead: read=%d sent=%d", counted, conn.sentTail.Load())
	}
	// Opening the client window lets queued bytes transmit and rearms the
	// duplicated source reader without consuming application-side upload data.
	conn.sendPermit.Store(1 + 2*goTransmitReadAhead)
	conn.sendPacketPermit.Store(1 << 16)
	conn.engine.spliceResume(conn)
	if conn.splice.readStalled || platform.interest&goInterestRead == 0 {
		t.Fatal("opening the client window did not resume socket reads")
	}
	conn.engine.spliceReadPeer(conn)
	if counted != int64(len(payload)) || conn.sentTail.Load() <= 1 {
		t.Fatalf("window reopening lost buffered data: read=%d sent=%d", counted, conn.sentTail.Load())
	}
}

func TestGoDownloadSpliceRejectsBeforeHandoff(t *testing.T) {
	for _, name := range []string{"handshake", "write_closed", "fin_requested", "writer_busy", "closed_source", "unsupported_platform", "engine_stopped"} {
		t.Run(name, func(t *testing.T) {
			conn, platform := newDownloadSpliceTestConn(t)
			source, _ := downloadSpliceSocketPair(t)
			switch name {
			case "handshake":
				conn.phase = goPhaseEngaged
			case "write_closed":
				conn.writeShut = true
			case "fin_requested":
				conn.finRequested = true
			case "writer_busy":
				conn.writeAccess.Lock()
				defer conn.writeAccess.Unlock()
			case "closed_source":
				source.Close()
			case "unsupported_platform":
				conn.engine.platformIO = platform.goPlatformIO
			case "engine_stopped":
				conn.engine.engineState.Store(goEngineExited)
			}
			called := false
			if conn.SpliceDownload(source, DownloadSpliceOptions{OnReadDone: func(error) { called = true }}) {
				t.Fatal("ineligible download splice accepted")
			}
			if called || conn.splicePending != nil || conn.splice != nil {
				t.Fatal("rejected handoff changed socket ownership")
			}
		})
	}
}

func TestGoDownloadSpliceCloseAndRegistrationFailure(t *testing.T) {
	for _, name := range []string{"close_pending", "close_engaged", "close_write_pending", "close_write_engaged", "register_failure", "stop_pending"} {
		t.Run(name, func(t *testing.T) {
			conn, platform := newDownloadSpliceTestConn(t)
			source, peer := downloadSpliceSocketPair(t)
			finished := make(chan error, 2)
			if !conn.SpliceDownload(source, DownloadSpliceOptions{OnReadDone: func(err error) { finished <- err }}) {
				t.Fatal("download splice refused")
			}
			if name == "close_engaged" || name == "close_write_engaged" {
				conn.engine.drainControlQueue()
			}
			switch name {
			case "close_write_pending", "close_write_engaged":
				conn.CloseWrite()
			case "register_failure":
				platform.registerErr = errors.New("registration failed")
			case "stop_pending":
				conn.engine.engineState.Store(goEngineExited)
				conn.engine.releaseControlQueue()
			default:
				conn.Close()
			}
			conn.engine.drainControlQueue()
			select {
			case err := <-finished:
				if err == nil {
					t.Fatal("failed/cancelled handoff returned success")
				}
			default:
				t.Fatal("failed/cancelled handoff did not finish its reader")
			}
			conn.engine.spliceDetach(conn, net.ErrClosed)
			select {
			case err := <-finished:
				t.Fatalf("completion called twice: %v", err)
			default:
			}
			if conn.splice != nil || conn.splicePending != nil || conn.spliced {
				t.Fatal("failed/cancelled handoff retained native socket references")
			}
			if _, err := source.Write([]byte{1}); err != nil {
				t.Fatal("native close closed original upload socket: ", err)
			}
			var data [1]byte
			if _, err := io.ReadFull(peer, data[:]); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestGoFullSpliceRetainsOwnershipTransfer(t *testing.T) {
	conn, _ := newDownloadSpliceTestConn(t)
	source, peer := downloadSpliceSocketPair(t)
	if !conn.Splice(&kernelSocket{TCPConn: source}, SpliceOptions{}) {
		t.Fatal("full splice refused")
	}
	conn.engine.drainControlQueue()
	if conn.splice == nil || conn.splice.downloadOnly {
		t.Fatal("full splice lost bidirectional ownership")
	}
	if _, err := source.Write([]byte{1}); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("full splice did not close the original socket: %v", err)
	}
	payload := []byte("full-splice upload")
	conn.engine.deliverInOrder(conn, payload)
	conn.engine.flushSpliceDirty()
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(peer, got); err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("full splice no longer forwards upload: %v", err)
	}
}

func TestGoDownloadSpliceCloseRace(t *testing.T) {
	for range 32 {
		conn, _ := newDownloadSpliceTestConn(t)
		source, _ := downloadSpliceSocketPair(t)
		var completions atomic.Int32
		var accepted bool
		var work sync.WaitGroup
		work.Add(2)
		go func() {
			defer work.Done()
			accepted = conn.SpliceDownload(source, DownloadSpliceOptions{OnReadDone: func(error) { completions.Add(1) }})
		}()
		go func() {
			defer work.Done()
			conn.Close()
		}()
		work.Wait()
		conn.engine.drainControlQueue()
		want := int32(0)
		if accepted {
			want = 1
		}
		if completions.Load() != want || conn.splice != nil || conn.splicePending != nil {
			t.Fatal("close race lost completion or retained an endpoint")
		}
	}
}

func TestGoKernelDownloadSplice(t *testing.T) {
	fixture := newKernelStackFixture(t, kernelStackConfig{mtu: 1500})
	for _, ipv6 := range []bool{false, true} {
		client, conn := fixture.pair(t, ipv6)
		source, peer := downloadSpliceSocketPair(t)
		finished := make(chan error, 1)
		var counted atomic.Int64
		if !conn.SpliceDownload(source, DownloadSpliceOptions{
			WriteCounters: []N.CountFunc{func(n int64) { counted.Add(n) }},
			OnReadDone:    func(err error) { finished <- err },
		}) {
			t.Fatal("download splice refused")
		}
		uploadDone := make(chan error, 1)
		go func() { _, err := io.Copy(source, conn); uploadDone <- err }()
		upload := []byte("normal upload through original socket")
		if _, err := client.Write(upload); err != nil {
			t.Fatal(err)
		}
		data := make([]byte, len(upload))
		if _, err := io.ReadFull(peer, data); err != nil || !bytes.Equal(data, upload) {
			t.Fatalf("normal upload: %v", err)
		}
		payload := kernelPayload(32768, 199)
		if _, err := peer.Write(payload); err != nil {
			t.Fatal(err)
		}
		peer.CloseWrite()
		got, err := io.ReadAll(client)
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("download through EOF: %v", err)
		}
		select {
		case err := <-finished:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("download completion waited for upload EOF")
		}
		if counted.Load() != int64(len(payload)) {
			t.Fatalf("download counter = %d", counted.Load())
		}
		if _, err := client.Write(upload); err != nil {
			t.Fatal("client upload stopped at download EOF: ", err)
		}
		if _, err := io.ReadFull(peer, data); err != nil || !bytes.Equal(data, upload) {
			t.Fatalf("normal upload after EOF: %v", err)
		}
		client.CloseWrite()
		select {
		case err := <-uploadDone:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("normal upload task did not receive client EOF")
		}
	}
}
