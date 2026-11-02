//go:build linux && !android

package tun

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/sagernet/sing-tun/gtcpip/header"
	N "github.com/sagernet/sing/common/network"
)

// Preserve the private native capability through the existing fault-injection
// wrapper. Memory-backed fixtures still decline through their underlying I/O.
func (p *kernelTCPIO) supportsTCPLocalJumbo() bool {
	queue, ok := p.goPlatformIO.(goTCPLocalJumboIO)
	return ok && queue.supportsTCPLocalJumbo()
}

type localJumboWireObservation struct {
	largest atomic.Int64
	invalid atomic.Bool
}

type localJumboKernelIO struct {
	goPlatformIO
	observation *localJumboWireObservation
}

func (p *localJumboKernelIO) supportsTCPLocalJumbo() bool {
	queue, ok := p.goPlatformIO.(goTCPLocalJumboIO)
	return ok && queue.supportsTCPLocalJumbo()
}

func (p *localJumboKernelIO) writeData(frame [][]byte, meta ForwardFrameMeta, owner *GoConn, segmentEnd uint64) error {
	packet := slices.Concat(frame...)
	parsed, valid := parseForwardPacket(packet)
	if !valid || len(packet) > 65535 || meta != (ForwardFrameMeta{}) || !goValidateTransportChecksum(&parsed) {
		p.observation.invalid.Store(true)
	}
	if valid {
		length := int64(len(header.TCP(parsed.transport).Payload()))
		for old := p.observation.largest.Load(); length > old; old = p.observation.largest.Load() {
			if p.observation.largest.CompareAndSwap(old, length) {
				break
			}
		}
	}
	return p.goPlatformIO.writeData(frame, meta, owner, segmentEnd)
}

func TestGoKernelLocalJumbo(t *testing.T) {
	if os.Geteuid() != 0 {
		if os.Getenv("XRAY_REQUIRE_NATIVE_TUN_TESTS") == "1" {
			t.Fatal("local-jumbo integration requires CAP_NET_ADMIN")
		}
		t.Skip("requires a disposable privileged Linux runner")
	}
	for _, congestion := range []string{"cubic", "bbr"} {
		for _, ipv6 := range []bool{false, true} {
			for _, writer := range []string{"write", "vector", "splice"} {
				for _, loseFirst := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/ipv6=%v/%s/loss=%v", congestion, ipv6, writer, loseFirst), func(t *testing.T) {
						observation := new(localJumboWireObservation)
						config := kernelStackConfig{mtu: 1500, congestion: congestion,
							stackFactory: func(options StackOptions) (Stack, error) {
								options.TCPLocalJumbo = true
								return NewGo(options)
							},
							prepareStack: func(stack *Go) {
								queues := stack.queueFactory
								stack.queueFactory = func(current *Go) ([]goPlatformIO, error) {
									platforms, err := queues(current)
									for index, platform := range platforms {
										platforms[index] = &localJumboKernelIO{goPlatformIO: platform, observation: observation}
									}
									return platforms, err
								}
							},
						}
						fixture, traffic := newKernelTCPFixture(t, config, kernelTCPConfig{})
						client, sender, conn := traffic.pair(t, fixture, ipv6, writer)
						if !conn.localJumboEnabled() {
							t.Fatal("plain host-local Linux flow did not enable explicit jumbo")
						}
						mss := conn.effectiveMSS.Load()
						var dropped atomic.Bool
						if loseFirst {
							traffic.setFilter(conn, func(event kernelTCPEvent) kernelTCPAction {
								if event.outgoing && event.length > int(mss) && dropped.CompareAndSwap(false, true) {
									return kernelTCPAction{drop: true}
								}
								return kernelTCPAction{}
							})
						}
						payload := kernelPayload(256<<10, 31)
						written := make(chan error, 1)
						go func() {
							if writer == "vector" {
								parts := make([][]byte, 0, len(payload)/8192)
								for offset := 0; offset < len(payload); offset += 8192 {
									parts = append(parts, payload[offset:min(offset+8192, len(payload))])
								}
								n, err := conn.WriteVector(parts)
								if err == nil && n != len(payload) {
									err = io.ErrShortWrite
								}
								written <- err
							} else {
								written <- kernelTCPWrite(sender, payload, false)
							}
						}()
						actual := make([]byte, len(payload))
						if _, err := io.ReadFull(client, actual); err != nil || !bytes.Equal(actual, payload) {
							t.Fatalf("local jumbo transfer: %v", err)
						}
						if err := kernelTCPResult(t, written, "local jumbo write"); err != nil {
							t.Fatal(err)
						}
						if observation.invalid.Load() || conn.effectiveMSS.Load() != mss {
							t.Fatal("jumbo changed checksums/metadata/MSS")
						}
						if loseFirst {
							if !dropped.Load() || conn.localJumboEnabled() {
								t.Fatal("lost jumbo was not exercised or did not trigger MSS fallback")
							}
						} else if observation.largest.Load() <= int64(mss) || !conn.localJumboEnabled() {
							t.Fatal("large packets were not delivered on the plain native TUN")
						}
						if err := N.CloseWrite(sender); err != nil {
							t.Fatal(err)
						}
						if tail, err := io.ReadAll(client); err != nil || len(tail) != 0 {
							t.Fatalf("download FIN: %d bytes, %v", len(tail), err)
						}
						if err := kernelTransfer(client, sender, kernelPayload(37, 17), false); err != nil {
							t.Fatal("independent upload after download FIN: ", err)
						}
					})
				}
			}
		}
	}
}
