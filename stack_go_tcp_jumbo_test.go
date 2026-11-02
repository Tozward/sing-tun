package tun

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"slices"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/sagernet/sing-tun/gtcpip/header"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
)

type localJumboTestFrame struct {
	packet []byte
	meta   ForwardFrameMeta
	err    error
}

type localJumboTestIO struct {
	goPlatformIO
	capable bool
	fail    error
	frames  []localJumboTestFrame
	access  sync.Mutex
}

func (p *localJumboTestIO) supportsTCPLocalJumbo() bool          { return p.capable }
func (*localJumboTestIO) transmitBacklogBelowBatch(*GoConn) bool { return true }
func (p *localJumboTestIO) writeFrame(frame [][]byte, meta ForwardFrameMeta) error {
	p.access.Lock()
	defer p.access.Unlock()
	err := p.fail
	p.fail = nil
	p.frames = append(p.frames, localJumboTestFrame{packet: slices.Concat(frame...), meta: meta, err: err})
	return err
}
func (p *localJumboTestIO) writeData(frame [][]byte, meta ForwardFrameMeta, _ *GoConn, _ uint64) error {
	return p.writeFrame(frame, meta)
}
func (p *localJumboTestIO) writePacket(packet []byte, meta ForwardFrameMeta) error {
	return p.writeFrame([][]byte{packet}, meta)
}

func newLocalJumboTestConn(t *testing.T, ipv6, timestamps bool) (*GoConn, *localJumboTestIO) {
	t.Helper()
	device := NewMemoryTun(MemoryTunOptions{MTU: 1500})
	options := Options{MTU: 1500,
		Inet4Address: []netip.Prefix{netip.MustParsePrefix("10.0.0.2/30")},
		Inet6Address: []netip.Prefix{netip.MustParsePrefix("fd01::2/126")},
	}
	stack, err := NewGo(StackOptions{Context: context.Background(), Tun: device, TunOptions: options,
		Logger: logger.NOP(), TCPCongestionControl: "reno", TCPLocalJumbo: true})
	if err != nil {
		t.Fatal(err)
	}
	platform := &localJumboTestIO{goPlatformIO: &goMemoryIO{stack: stack, tun: device}, capable: true}
	engine := newGoEngine(stack, platform, 1)
	peer, local := M.ParseSocksaddr("10.0.0.2:12000"), M.ParseSocksaddr("203.0.113.1:443")
	ipVersion := uint8(4)
	if ipv6 {
		peer, local = M.ParseSocksaddr("[fd01::2]:12000"), M.ParseSocksaddr("[2001:db8::1]:443")
		ipVersion = 6
	}
	key := flowKey{protocol: uint8(header.TCPProtocolNumber), source: peer.AddrPort(), destination: local.AddrPort()}
	conn := new(GoConn)
	conn.initialize(engine, key, peer, local)
	conn.ipVersion, conn.timestampsEnabled = ipVersion, timestamps
	mss := 1500 - goNetworkHeaderLength(ipVersion) - header.TCPMinimumSize
	if timestamps {
		mss -= goTimestampOptionLength
	}
	conn.effectiveMSS.Store(uint32(mss))
	engine.initializeSession(conn, engine.coarseTime.Load(), 65535)
	conn.phase, conn.state = goPhaseEstablished, goTCPEstablished
	conn.congestionWindow = 4096
	conn.sendPacketPermit.Store(4096)
	conn.sendPermit.Store(1 + 1<<20)
	engine.flows[key] = conn
	close(conn.establishedSignal)
	t.Cleanup(func() {
		engine.detachConn(conn, net.ErrClosed, goDeathImmediate)
		engine.engineState.Store(goEngineExited)
		engine.releaseControlQueue()
		engine.reapDying()
		device.Close()
	})
	return conn, platform
}

func TestGoLocalJumboEligibility(t *testing.T) {
	conn, platform := newLocalJumboTestConn(t, false, true)
	stack := conn.engine.stack
	for _, test := range []struct {
		name    string
		peer    string
		enabled bool
		capable bool
		want    bool
	}{
		{"local4", "10.0.0.2", true, true, true},
		{"local6", "fd01::2", true, true, true},
		{"neighbor_not_local", "10.0.0.1", true, true, false},
		{"same_ipv6_prefix", "fd01::1", true, true, false},
		{"forwarded", "192.0.2.100", true, true, false},
		{"off", "10.0.0.2", false, true, false},
		{"unsupported_device", "10.0.0.2", true, false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			stack.tcpLocalJumbo, platform.capable = test.enabled, test.capable
			if got := stack.localJumboPeer(netip.MustParseAddr(test.peer), platform); got != test.want {
				t.Fatalf("eligibility = %v, want %v", got, test.want)
			}
		})
	}
	stack.tcpLocalJumbo = true
	if stack.localJumboPeer(netip.MustParseAddr("10.0.0.2"), platform.goPlatformIO) {
		t.Fatal("MemoryTun enabled local jumbo")
	}
}

func TestGoLocalJumboFrameChecksumsAndBounds(t *testing.T) {
	for _, ipv6 := range []bool{false, true} {
		for _, timestamps := range []bool{false, true} {
			t.Run(fmt.Sprintf("ipv6=%v/timestamps=%v", ipv6, timestamps), func(t *testing.T) {
				conn, platform := newLocalJumboTestConn(t, ipv6, timestamps)
				localMSS := conn.engine.localMSS(conn.ipVersion)
				for _, mss := range []uint32{1, 536, conn.effectiveMSS.Load(), 8191, 32767, 65483, 65463, 65535} {
					conn.effectiveMSS.Store(mss)
					length := conn.frameLength(100000, mss)
					if length > goGSOMaxPayload || length < min(int(mss), goGSOMaxPayload) {
						t.Fatalf("MSS %d: aggregate payload %d", mss, length)
					}
					payload := bytes.Repeat([]byte{0x57}, length)
					conn.transmitStore.reserve(1, length)
					conn.transmitStore.writeAt(1, payload)
					segment := goSegment{offset: 1, length: length, flags: header.TCPFlagAck | header.TCPFlagPsh,
						sackBlocks: []goSackBlock{{start: 1, end: 3}, {start: 5, end: 7}, {start: 9, end: 11}}}
					if err := conn.transmitFrame(&segment); err != nil {
						t.Fatal(err)
					}
					frame := platform.frames[len(platform.frames)-1]
					parsed, valid := parseForwardPacket(frame.packet)
					if !valid || !goValidateTransportChecksum(&parsed) || len(frame.packet) > 65535 || frame.meta != (ForwardFrameMeta{}) {
						t.Fatalf("invalid plain packet: length=%d valid=%v meta=%+v", len(frame.packet), valid, frame.meta)
					}
					if !ipv6 && !header.IPv4(frame.packet).IsChecksumValid() {
						t.Fatal("invalid IPv4 header checksum")
					}
					if !bytes.Equal(header.TCP(parsed.transport).Payload(), payload) {
						t.Fatal("aggregate payload changed")
					}
					if conn.effectiveMSS.Load() != mss || conn.engine.localMSS(conn.ipVersion) != localMSS {
						t.Fatal("local jumbo changed negotiated or advertised MSS")
					}
					conn.frameLimit.Store(3 * mss)
					if got := conn.frameLength(100000, mss); got > int(3*mss) {
						t.Fatalf("ignored pacing frame limit: %d", got)
					}
					conn.frameLimit.Store(0)
					conn.disableLocalJumbo()
					if got := conn.frameLength(100000, mss); got != min(int(mss), goGSOMaxPayload) {
						t.Fatalf("fallback length %d, want MSS %d", got, mss)
					}
					conn.localJumboDisabled.Store(false)
				}
			})
		}
	}
}

func TestGoLocalJumboLargeMSSRetransmitBound(t *testing.T) {
	for _, ipv6 := range []bool{false, true} {
		t.Run(fmt.Sprintf("ipv6=%v", ipv6), func(t *testing.T) {
			conn, platform := newLocalJumboTestConn(t, ipv6, true)
			conn.effectiveMSS.Store(65535)
			payload := bytes.Repeat([]byte{0x64}, 62000)
			if _, err := conn.WriteVector([][]byte{payload}); err != nil {
				t.Fatal(err)
			}
			conn.engine.transmitOnEngine(conn, 0)
			conn.engine.drainDescriptors(conn)
			if n := conn.engine.transmitRetransmit(conn, 1, len(payload), false); n != goGSOMaxPayload {
				t.Fatalf("bounded retransmit length: %d", n)
			}
			if conn.localJumboEnabled() {
				t.Fatal("retransmission did not disable local aggregation")
			}
			for _, frame := range platform.frames {
				parsed, valid := parseForwardPacket(frame.packet)
				if !valid || len(frame.packet) > 65535 || !goValidateTransportChecksum(&parsed) {
					t.Fatalf("invalid large-MSS packet: %d bytes", len(frame.packet))
				}
			}
			if got := conn.frameLength(100000, 65535); got != goGSOMaxPayload {
				t.Fatalf("fallback allowed an oversized new packet: %d", got)
			}
		})
	}
}

func TestGoLocalJumboRejectedWriteRecoversAtMSS(t *testing.T) {
	for _, writeError := range []error{syscall.EMSGSIZE, syscall.EINVAL, syscall.ENOBUFS, errGoFrameDropped} {
		t.Run(writeError.Error(), func(t *testing.T) {
			conn, platform := newLocalJumboTestConn(t, false, true)
			platform.fail = fmt.Errorf("device: %w", writeError)
			if writeError == errGoFrameDropped {
				platform.fail = writeError
			}
			payload := bytes.Repeat([]byte{0x35}, 16<<10)
			mss := conn.effectiveMSS.Load()
			if n, err := conn.WriteVector([][]byte{payload[:8192], payload[8192:]}); err != nil || n != len(payload) {
				t.Fatalf("buffering rejected aggregate: %d %v", n, err)
			}
			conn.engine.transmitOnEngine(conn, 0)
			if conn.localJumboEnabled() || conn.effectiveMSS.Load() != mss || conn.err != nil {
				t.Fatal("rejected jumbo did not safely disable aggregation")
			}
			conn.engine.handleDroppedFrames(conn)
			if len(platform.frames) < 2 {
				t.Fatal("rejected jumbo did not enter retransmission recovery")
			}
			for _, frame := range platform.frames[1:] {
				parsed, valid := parseForwardPacket(frame.packet)
				if !valid || len(header.TCP(parsed.transport).Payload()) > int(mss) || !goValidateTransportChecksum(&parsed) {
					t.Fatal("recovery emitted an invalid or oversized retransmission")
				}
			}
		})
	}
	conn, _ := newLocalJumboTestConn(t, false, true)
	if err := conn.localJumboWriteResult(syscall.EIO, 10000); err != syscall.EIO || !conn.localJumboEnabled() {
		t.Fatal("unrelated device error was hidden")
	}
	conn.disableLocalJumbo()
	if err := conn.localJumboWriteResult(syscall.ENOBUFS, 10000); err != errGoFrameDropped {
		t.Fatal("a jumbo already in flight during fallback must still recover at the MSS")
	}
}

func TestGoLocalJumboConcurrentFallbackKeepsPacketBound(t *testing.T) {
	conn, _ := newLocalJumboTestConn(t, true, true)
	conn.effectiveMSS.Store(32767)
	var writers sync.WaitGroup
	writers.Add(1)
	go func() {
		defer writers.Done()
		for range 10000 {
			conn.localJumboDisabled.Store(true)
			conn.localJumboDisabled.Store(false)
		}
	}()
	for range 10000 {
		if length := conn.frameLength(100000, 32767); length+goHeaderScratchSize > 65535 {
			t.Errorf("fallback race expanded payload beyond the IP limit: %d", length)
			break
		}
	}
	writers.Wait()
}

func TestGoLocalJumboValidatedPacketTooBigDisablesAggregation(t *testing.T) {
	conn, platform := newLocalJumboTestConn(t, false, true)
	if _, err := conn.WriteVector([][]byte{bytes.Repeat([]byte{0x42}, 16000)}); err != nil {
		t.Fatal(err)
	}
	conn.engine.transmitOnEngine(conn, 0)
	quoted, valid := parseEmbedded(platform.frames[0].packet)
	if !valid {
		t.Fatal("cannot quote aggregate")
	}
	invalid := quoted
	invalid.payload = slices.Clone(quoted.payload)
	header.TCP(invalid.payload).SetSequenceNumber(conn.sendISN + 1<<24)
	conn.engine.inputPacketTooBig(&invalid, 1500)
	if !conn.localJumboEnabled() {
		t.Fatal("unrelated quoted sequence disabled aggregation")
	}
	mss := conn.effectiveMSS.Load()
	conn.engine.inputPacketTooBig(&quoted, 1500)
	if conn.localJumboEnabled() || conn.effectiveMSS.Load() != mss {
		t.Fatal("valid MTU feedback must disable aggregation even at the current path MTU")
	}
}

func TestGoWriteVectorBorrowsAndPreservesOrder(t *testing.T) {
	conn, platform := newLocalJumboTestConn(t, false, true)
	parts := [][]byte{bytes.Repeat([]byte{0x31}, 8191), nil, bytes.Repeat([]byte{0x32}, 8193), bytes.Repeat([]byte{0x33}, 15000)}
	payload := bytes.Join(parts, nil)
	if n, err := conn.WriteVector(parts); err != nil || n != len(payload) {
		t.Fatalf("vector write: %d %v", n, err)
	}
	conn.engine.transmitOnEngine(conn, 0)
	for _, part := range parts {
		clear(part)
	}
	stored := bytes.Join(conn.transmitStore.appendRuns(nil, 1, len(payload)), nil)
	if !bytes.Equal(stored, payload) {
		t.Fatal("borrowed slices were retained after return")
	}
	var actual []byte
	for _, frame := range platform.frames {
		parsed, valid := parseForwardPacket(frame.packet)
		if !valid || frame.meta != (ForwardFrameMeta{}) {
			t.Fatal("invalid vector output")
		}
		actual = append(actual, header.TCP(parsed.transport).Payload()...)
	}
	if !bytes.Equal(actual, payload) {
		t.Fatal("vector output reordered or lost data")
	}
	conn.engine.drainDescriptors(conn)
	previous := uint64(1)
	for _, descriptor := range conn.scoreboard.entries {
		if descriptor.endOffset-previous > uint64(conn.effectiveMSS.Load()) {
			t.Fatal("aggregate changed per-MSS congestion accounting")
		}
		previous = descriptor.endOffset
	}
	if previous != 1+uint64(len(payload)) {
		t.Fatal("descriptor accounting lost bytes")
	}
}

func TestGoWriteVectorPartialDeadline(t *testing.T) {
	conn, _ := newLocalJumboTestConn(t, false, true)
	conn.transmitCapacity = 4096
	if err := conn.SetWriteDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte{0x21}, 4097)
	n, err := conn.WriteVector([][]byte{payload[:2000], payload[2000:]})
	if n != 4096 || !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("partial deadline: n=%d err=%v", n, err)
	}
	conn.engine.transmitOnEngine(conn, 0)
	if conn.bufferedTail.Load() != 4097 || conn.transmittedTail.Load() != 4097 {
		t.Fatal("copied prefix was not published before backpressure")
	}
}

func TestGoWriteVectorCloseUnblocksPartialWrite(t *testing.T) {
	conn, _ := newLocalJumboTestConn(t, false, true)
	conn.transmitCapacity = 4096
	type result struct {
		n   int
		err error
	}
	done := make(chan result, 1)
	go func() {
		n, err := conn.WriteVector([][]byte{make([]byte, 2048), make([]byte, 8192)})
		done <- result{n, err}
	}()
	deadline := time.Now().Add(time.Second)
	for conn.bufferedTail.Load() != 4097 {
		if time.Now().After(deadline) {
			t.Fatal("writer did not reach backpressure")
		}
		time.Sleep(time.Millisecond)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-done:
		if got.n != 4096 || !errors.Is(got.err, net.ErrClosed) {
			t.Fatalf("close interrupted vector: n=%d err=%v", got.n, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("close left vector writer blocked")
	}
}

func TestGoWriteVectorConcurrentCallsDoNotInterleave(t *testing.T) {
	conn, _ := newLocalJumboTestConn(t, false, true)
	a := [][]byte{bytes.Repeat([]byte{'A'}, 1000), bytes.Repeat([]byte{'a'}, 1000)}
	b := [][]byte{bytes.Repeat([]byte{'B'}, 1000), bytes.Repeat([]byte{'b'}, 1000)}
	done := make(chan error, 2)
	for _, parts := range [][][]byte{a, b} {
		go func() {
			n, err := conn.WriteVector(parts)
			if err == nil && n != 2000 {
				err = fmt.Errorf("short vector write: %d", n)
			}
			done <- err
		}()
	}
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	stored := bytes.Join(conn.transmitStore.appendRuns(nil, 1, 4000), nil)
	ab := bytes.Join(append(slices.Clone(a), b...), nil)
	ba := bytes.Join(append(slices.Clone(b), a...), nil)
	if !bytes.Equal(stored, ab) && !bytes.Equal(stored, ba) {
		t.Fatal("concurrent vector calls interleaved their buffers")
	}
}
