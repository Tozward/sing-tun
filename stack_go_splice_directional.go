package tun

import (
	"net"
	"reflect"
	"syscall"

	N "github.com/sagernet/sing/common/network"
)

// UploadSpliceOptions accounts for bytes consumed from the client's receive
// storage and written to the target socket. OnWriteDone runs once, after client
// EOF and all queued upload bytes have been written, or on failure. It must not
// block the engine and does not report whole-connection completion.
type UploadSpliceOptions struct {
	ReadCounters []N.CountFunc
	OnWriteDone  N.CloseHandlerFunc
}

// goDirectionalSpliceState is protected by GoConn.access. The original must be
// the same comparable syscall.Conn instance in both calls, not another wrapper
// or a recycled descriptor. The caller keeps that original alive until both
// accepted directions finish. Active socket state belongs only to the engine.
type goDirectionalSpliceState struct {
	original        syscall.Conn
	uploadClaimed   bool
	downloadClaimed bool
	uploadPending   *goDirectionalSpliceRequest
	downloadPending *goDirectionalSpliceRequest
	active          bool
	stopped         bool
}

type goDirectionalSpliceRequest struct {
	socket    goSocket
	hasSocket bool
	upload    bool
	counters  []N.CountFunc
	done      N.CloseHandlerFunc
}

func (r *goDirectionalSpliceRequest) finish(err error) {
	if r.hasSocket {
		r.socket.close()
		r.hasSocket = false
	}
	if done := r.done; done != nil {
		r.done = nil
		done(err)
	}
}

// SpliceUpload permanently reserves this GoConn's read side and writes its
// client upload directly to a duplicated target socket. The caller must have
// stopped reading this GoConn and drained any bytes held outside it. A pending
// reader makes this method decline without consuming data or taking ownership.
// The target must be a nonblocking stream socket. Its original remains owned by
// the caller; ordinary GoConn writes and target reads stay available until their
// respective direction is handed off separately. Both directional calls require
// the same comparable syscall.Conn instance. Engine IO ignores socket deadlines;
// close this GoConn to cancel an accepted handoff.
func (c *GoConn) SpliceUpload(target syscall.Conn, options UploadSpliceOptions) bool {
	if !c.directionalSourceSupported(target) || !c.readAccess.TryLock() {
		return false
	}
	defer c.readAccess.Unlock()
	return c.requestDirectionalSplice(target, true, options.ReadCounters, options.OnWriteDone)
}

func (c *GoConn) directionalSourceSupported(source syscall.Conn) bool {
	return source != nil && goSpliceDuplicatesSocket && c.engine.platformIO.supportsSockets() &&
		reflect.TypeOf(source).Comparable()
}

func (c *GoConn) directionalSpliceEligible(source syscall.Conn, upload bool) bool {
	if c.dead || c.userClosed || c.abort || c.phase != goPhaseEstablished ||
		c.splicePending != nil || c.engine.engineState.Load() == goEngineExited {
		return false
	}
	state := c.directionalSplice
	if state == nil {
		if c.spliced {
			return false
		}
	} else if state.stopped || state.original != source || upload && state.uploadClaimed || !upload && state.downloadClaimed {
		return false
	}
	if upload {
		return !c.readShut
	}
	return !c.writeShut && !c.finRequested
}

func (c *GoConn) requestDirectionalSplice(source syscall.Conn, upload bool, counters []N.CountFunc, done N.CloseHandlerFunc) bool {
	c.access.Lock()
	eligible := c.directionalSpliceEligible(source, upload)
	c.access.Unlock()
	if !eligible {
		return false
	}
	socket, err := goDirectionalSocket(source)
	if err != nil {
		return false
	}
	request := &goDirectionalSpliceRequest{
		socket: socket, hasSocket: true, upload: upload, counters: counters, done: done,
	}
	c.access.Lock()
	installed := c.directionalSpliceEligible(source, upload)
	if installed {
		if c.directionalSplice == nil {
			c.directionalSplice = &goDirectionalSpliceState{original: source}
		}
		state := c.directionalSplice
		if upload {
			state.uploadClaimed = true
			state.uploadPending = request
		} else {
			state.downloadClaimed = true
			state.downloadPending = request
			c.writeShut = true
		}
		c.spliced = true
	}
	c.access.Unlock()
	if !installed {
		// A declined request must not invoke its completion callback.
		socket.close()
		return false
	}
	c.engine.postMessage(&c.spliceMessage)
	return true
}

func (e *goEngine) handleDirectionalSpliceEngage(conn *GoConn) bool {
	conn.access.Lock()
	state := conn.directionalSplice
	if state == nil {
		conn.access.Unlock()
		return false
	}
	upload, download := state.uploadPending, state.downloadPending
	state.uploadPending, state.downloadPending = nil, nil
	cancelled := state.stopped || conn.userClosed || conn.abort
	uploadCancelled := cancelled || conn.readShut || conn.dead && conn.deathClass != goDeathFinLinger
	downloadCancelled := cancelled || conn.finRequested || conn.dead
	conn.access.Unlock()
	for _, request := range []*goDirectionalSpliceRequest{upload, download} {
		if request == nil {
			continue
		}
		conn.access.Lock()
		stopped := state.stopped || conn.userClosed || conn.abort || conn.dead && (!request.upload || conn.deathClass != goDeathFinLinger)
		conn.access.Unlock()
		if stopped || request.upload && uploadCancelled || !request.upload && downloadCancelled {
			request.finish(net.ErrClosed)
			continue
		}
		splice := conn.splice
		if splice != nil && !splice.directional {
			// Directional requests never inherit a full owner's socket, even if
			// an invalid pending state reaches the engine.
			request.finish(net.ErrClosed)
			e.abortConn(conn, net.ErrClosed)
			continue
		}
		if splice == nil {
			splice = &goSpliceStream{conn: conn, socket: request.socket, directional: true, downloadOnly: true}
			request.hasSocket = false
			splice.token = e.allocateSpliceToken(conn, nil)
			conn.splice = splice
			conn.access.Lock()
			state.active = true
			conn.access.Unlock()
		}
		if request.upload {
			splice.uploadEnabled = true
			splice.downloadOnly = false
			splice.options.ReadCounters = request.counters
			splice.onWriteDone = request.done
		} else {
			splice.downloadEnabled = true
			splice.options.WriteCounters = request.counters
			splice.onReadDone = request.done
		}
		request.done = nil
		request.finish(nil)
		if request.upload {
			e.spliceFlushUpload(conn)
		} else {
			e.spliceSetInterest(conn, splice.interest|goInterestRead)
		}
	}
	e.updateDirectionalSpliced(conn)
	return true
}

func (e *goEngine) updateDirectionalSpliced(conn *GoConn) {
	conn.access.Lock()
	state := conn.directionalSplice
	conn.spliced = state.active || state.uploadPending != nil || state.downloadPending != nil
	conn.access.Unlock()
}

// releaseDirectionalPending can also run after the engine exits. It never
// touches active engine state; all requests are removed under access first.
func (e *goEngine) releaseDirectionalPending(conn *GoConn, err error, stop bool) {
	conn.access.Lock()
	state := conn.directionalSplice
	if state == nil {
		conn.access.Unlock()
		return
	}
	if stop {
		state.stopped = true
	}
	upload, download := state.uploadPending, state.downloadPending
	state.uploadPending, state.downloadPending = nil, nil
	conn.spliced = state.active
	conn.access.Unlock()
	if upload != nil {
		upload.finish(err)
	}
	if download != nil {
		download.finish(err)
	}
}

func (e *goEngine) finishDirectionalUpload(conn *GoConn, err error) {
	splice := conn.splice
	splice.uploadEnabled = false
	splice.downloadOnly = true
	splice.uploadClosed = true
	done := splice.onWriteDone
	splice.onWriteDone = nil
	e.spliceSetInterest(conn, splice.interest&^goInterestWrite)
	e.directionalMaybeDetach(conn)
	if done != nil {
		done(err)
	}
}

func (e *goEngine) finishDirectionalDownload(conn *GoConn, err error) {
	splice := conn.splice
	splice.downloadEnabled = false
	splice.downloadClosed = true
	done := splice.onReadDone
	splice.onReadDone = nil
	conn.access.Lock()
	conn.finRequested = true
	conn.access.Unlock()
	conn.finPending = true
	e.spliceSetInterest(conn, splice.interest&^goInterestRead)
	e.directionalMaybeDetach(conn)
	conn.transmitPublished()
	e.maybeSendFin(conn)
	if done != nil {
		done(err)
	}
}

func (e *goEngine) directionalMaybeDetach(conn *GoConn) {
	splice := conn.splice
	if splice == nil || splice.uploadEnabled || splice.downloadEnabled {
		return
	}
	if splice.dirty {
		e.spliceUnlinkDirty(conn)
	}
	conn.splice = nil
	if splice.pollRegistered {
		e.platformIO.unregisterSocket(&splice.socket)
	}
	e.releaseSpliceToken(splice.token)
	splice.socket.close()
	conn.access.Lock()
	conn.directionalSplice.active = false
	conn.access.Unlock()
	e.updateDirectionalSpliced(conn)
}

func (e *goEngine) closeDirectionalWrite(conn *GoConn) bool {
	conn.access.Lock()
	state := conn.directionalSplice
	if state == nil {
		conn.access.Unlock()
		return false
	}
	pending := state.downloadPending
	state.downloadPending = nil
	conn.access.Unlock()
	if pending != nil {
		pending.finish(net.ErrClosed)
	}
	if splice := conn.splice; splice != nil && splice.downloadEnabled {
		e.finishDirectionalDownload(conn, net.ErrClosed)
	}
	e.updateDirectionalSpliced(conn)
	return true
}

func (e *goEngine) closeDirectionalRead(conn *GoConn) {
	conn.access.Lock()
	state := conn.directionalSplice
	if state == nil {
		conn.access.Unlock()
		return
	}
	if conn.dead && conn.readShut && state.uploadClaimed {
		// Cancellation also discards retained FIN-linger receive storage.
		// CloseRead may have queued this message before the final ACK.
		conn.drainable = false
	}
	pending := state.uploadPending
	state.uploadPending = nil
	conn.access.Unlock()
	if pending != nil {
		pending.finish(net.ErrClosed)
	}
	if splice := conn.splice; splice != nil && splice.uploadEnabled {
		e.finishDirectionalUpload(conn, net.ErrClosed)
	}
	e.updateDirectionalSpliced(conn)
}
