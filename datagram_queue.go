package quic

import (
	"context"
	"sync"

	"github.com/refraction-networking/uquic/internal/utils"
	"github.com/refraction-networking/uquic/internal/utils/ringbuffer"
	"github.com/refraction-networking/uquic/internal/wire"
)

const (
	// [VPP] VPP_DGRAM_QUEUE_V1: it was 32. A caller that orders its own
	// datagrams (VPP's FQ-CoDel: a call's packet ahead of an upload's) had
	// that order undone here: up to 32 full datagrams in front of the one it
	// had just let pass (38 KB: 300 ms on a 1 Mbit/s uplink). With 2, Add
	// blocks at once and what waits, waits in the caller's queue.
	maxDatagramSendQueueLen = 2
	// [VPP] VPP_DGRAM_URGENT_V1: datagrams that go ahead of the queue
	// (AddUrgent) wait in a queue of their own of this length; AddUrgent
	// blocks when it is full
	maxDatagramUrgentLen   = 8
	maxDatagramRcvQueueLen = 128
)

type datagramQueue struct {
	sendMx    sync.Mutex
	sendQueue ringbuffer.RingBuffer[*wire.DatagramFrame]
	urgent    ringbuffer.RingBuffer[*wire.DatagramFrame] // [VPP] VPP_DGRAM_URGENT_V1: sent before sendQueue
	peekedUrg bool                                       // the frame of the last Peek came from urgent
	sentUrg   chan struct{}                              // notifies AddUrgent that an urgent datagram was dequeued
	sent      chan struct{}                              // used to notify Add that a datagram was dequeued

	rcvMx    sync.Mutex
	rcvQueue [][]byte
	rcvd     chan struct{} // used to notify Receive that a new datagram was received

	closeErr error
	closed   chan struct{}

	hasData func()

	logger utils.Logger
}

func newDatagramQueue(hasData func(), logger utils.Logger) *datagramQueue {
	return &datagramQueue{
		hasData: hasData,
		rcvd:    make(chan struct{}, 1),
		sent:    make(chan struct{}, 1),
		sentUrg: make(chan struct{}, 1),
		closed:  make(chan struct{}),
		logger:  logger,
	}
}

// Add queues a new DATAGRAM frame for sending.
// Up to maxDatagramSendQueueLen DATAGRAM frames will be queued.
// Once that limit is reached, Add blocks until the queue size has reduced.
func (h *datagramQueue) Add(f *wire.DatagramFrame) error {
	h.sendMx.Lock()

	for {
		if h.sendQueue.Len() < maxDatagramSendQueueLen {
			h.sendQueue.PushBack(f)
			h.sendMx.Unlock()
			h.hasData()
			return nil
		}
		select {
		case <-h.sent: // drain the queue so we don't loop immediately
		default:
		}
		h.sendMx.Unlock()
		select {
		case <-h.closed:
			return h.closeErr
		case <-h.sent:
		}
		h.sendMx.Lock()
	}
}

// AddUrgent ([VPP] VPP_DGRAM_URGENT_V1) queues a DATAGRAM frame ahead of
// those queued with Add, behind the urgent ones before it (their order is
// kept). Up to maxDatagramUrgentLen wait; then AddUrgent blocks, as Add does.
func (h *datagramQueue) AddUrgent(f *wire.DatagramFrame) error {
	h.sendMx.Lock()
	for {
		if h.urgent.Len() < maxDatagramUrgentLen {
			h.urgent.PushBack(f)
			h.sendMx.Unlock()
			h.hasData()
			return nil
		}
		select {
		case <-h.sentUrg:
		default:
		}
		h.sendMx.Unlock()
		select {
		case <-h.closed:
			return h.closeErr
		case <-h.sentUrg:
		}
		h.sendMx.Lock()
	}
}

// Peek gets the next DATAGRAM frame for sending.
// If actually sent out, Pop needs to be called before the next call to Peek.
func (h *datagramQueue) Peek() *wire.DatagramFrame {
	h.sendMx.Lock()
	defer h.sendMx.Unlock()
	if !h.urgent.Empty() { // [VPP] VPP_DGRAM_URGENT_V1
		h.peekedUrg = true
		return h.urgent.PeekFront()
	}
	h.peekedUrg = false
	if h.sendQueue.Empty() {
		return nil
	}
	return h.sendQueue.PeekFront()
}

func (h *datagramQueue) Pop() {
	h.sendMx.Lock()
	defer h.sendMx.Unlock()
	if h.peekedUrg { // [VPP] VPP_DGRAM_URGENT_V1: Pop takes the queue Peek took its frame from
		_ = h.urgent.PopFront()
		select {
		case h.sentUrg <- struct{}{}:
		default:
		}
		return
	}
	_ = h.sendQueue.PopFront()
	select {
	case h.sent <- struct{}{}:
	default:
	}
}

// HandleDatagramFrame handles a received DATAGRAM frame.
func (h *datagramQueue) HandleDatagramFrame(f *wire.DatagramFrame) {
	data := make([]byte, len(f.Data))
	copy(data, f.Data)
	var queued bool
	h.rcvMx.Lock()
	if len(h.rcvQueue) < maxDatagramRcvQueueLen {
		h.rcvQueue = append(h.rcvQueue, data)
		queued = true
		select {
		case h.rcvd <- struct{}{}:
		default:
		}
	}
	h.rcvMx.Unlock()
	if !queued && h.logger.Debug() {
		h.logger.Debugf("Discarding received DATAGRAM frame (%d bytes payload)", len(f.Data))
	}
}

// Receive gets a received DATAGRAM frame.
func (h *datagramQueue) Receive(ctx context.Context) ([]byte, error) {
	for {
		h.rcvMx.Lock()
		if len(h.rcvQueue) > 0 {
			data := h.rcvQueue[0]
			h.rcvQueue = h.rcvQueue[1:]
			h.rcvMx.Unlock()
			return data, nil
		}
		h.rcvMx.Unlock()
		select {
		case <-h.rcvd:
			continue
		case <-h.closed:
			return nil, h.closeErr
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (h *datagramQueue) CloseWithError(e error) {
	h.closeErr = e
	close(h.closed)
}
