package tun

import (
	"context"
	"net"
	"os"
)

// WriteVector writes borrowed byte slices in order, entering the writer once.
// It copies bytes into the existing retransmission store just as Write does;
// no slice is retained after return. A partial result counts bytes across all
// slices. Each bounded copied prefix is published before waiting for space.
func (c *GoConn) WriteVector(buffers [][]byte) (int, error) {
	if err := c.awaitHandshake(context.Background(), c.writeDeadline.Wait()); err != nil {
		return 0, err
	}
	defer c.engine.reapAfterExit()
	c.writeAccess.Lock()
	defer c.writeAccess.Unlock()
	if !c.enterWriter() {
		return 0, net.ErrClosed
	}
	defer c.exitWriter()
	total, index, offset := 0, 0, 0
	for index < len(buffers) {
		if offset == len(buffers[index]) {
			index++
			offset = 0
			continue
		}
		c.access.Lock()
		dead, writeShut, capacity := c.dead, c.writeShut, c.transmitCapacity
		c.access.Unlock()
		if dead {
			return total, c.closeError()
		}
		if writeShut {
			return total, net.ErrClosed
		}
		select {
		case <-c.writeDeadline.Wait():
			return total, os.ErrDeadlineExceeded
		default:
		}
		budget := min(c.writeBudget(0, capacity), goTransmitReadAhead)
		if budget <= 0 {
			if err := c.parkWriter(1); err != nil {
				return total, err
			}
			continue
		}
		buffered := c.bufferedTail.Load()
		for index < len(buffers) && budget > 0 {
			length := min(len(buffers[index])-offset, budget)
			if length > 0 {
				c.transmitStore.reserve(buffered, length)
				c.transmitStore.writeAt(buffered, buffers[index][offset:offset+length])
				buffered += uint64(length)
				total += length
				budget -= length
				offset += length
			}
			if offset == len(buffers[index]) {
				index++
				offset = 0
			}
		}
		c.publishBuffered(buffered)
	}
	return total, nil
}
