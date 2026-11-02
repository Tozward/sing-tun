package tun

import (
	"errors"
	"net/netip"
	"syscall"
)

// This private capability exists only for native plain Linux TUN queues. In
// particular, memory devices, forwarded packet writers and virtio/GSO devices
// retain their existing packet-size contracts.
type goTCPLocalJumboIO interface {
	supportsTCPLocalJumbo() bool
}

func (s *Go) localJumboPeer(peer netip.Addr, platform goPlatformIO) bool {
	if !s.tcpLocalJumbo {
		return false
	}
	queue, ok := platform.(goTCPLocalJumboIO)
	if !ok || !queue.supportsTCPLocalJumbo() {
		return false
	}
	for _, address := range s.localJumboAddresses {
		if peer == address {
			return true
		}
	}
	return false
}

func (c *GoConn) localJumboEnabled() bool {
	return c.localJumbo && !c.localJumboDisabled.Load()
}

func (c *GoConn) disableLocalJumbo() {
	if c.localJumbo {
		c.localJumboDisabled.Store(true)
	}
}

func (c *GoConn) localJumboWriteResult(err error, length int) error {
	if err == nil || !c.localJumbo || length <= int(c.effectiveMSS.Load()) {
		return err
	}
	if err == errGoFrameDropped || errors.Is(err, syscall.EMSGSIZE) || errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.ENOBUFS) {
		// Descriptors already account for the aggregate as normal-MSS segments.
		// Reuse dropped-frame recovery to retransmit those bytes at the MSS;
		// never retry the rejected oversized packet in a busy loop.
		c.localJumboDisabled.Store(true)
		return errGoFrameDropped
	}
	return err
}
