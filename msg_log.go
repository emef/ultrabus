package ultrabus

import (
	"sync"

	"github.com/emef/ultrabus/pb"
)

type MessageLogCursor interface {
	// True if the cursor has a next offset to traverse to
	HasNext() bool

	// Returns the next message and moves cursor forward in the log
	Next() (*pb.MessageWithOffset, error)

	// Returns the current offset of the cursor
	Pos() int64

	// Seek to the given offset
	Seek(offset int64) error
}

type MessageLog interface {
	// Append the message to the log, returning the offset of this message
	Append(message *pb.Message) WriteReceipt

	// Create a cursor at the start of the log
	CursorStart() (MessageLogCursor, error)

	// Create a cursor at the end of the log
	CursorEnd() (MessageLogCursor, error)

	// Create a cursor from a given offset
	CursorAt(offset int64) (MessageLogCursor, error)

	// Current smallest offset in the log
	FirstOffset() (int64, error)

	// Current largest offset in the log
	LastOffset() (int64, error)
}

// When a message is being written a receipt is returned. This
// receipt acts like a "future" and resolves when the message is
// eventually processed successfully or not.
type WriteReceipt interface {
	// Channel to wait on for receipt resolution
	Done() chan interface{}

	// Read the receipt, if receipt hasn't been written this will
	// return an error.
	Read() (int64, error)
}

type inMemoryMessageLog struct {
	lock     sync.RWMutex
	messages []*pb.MessageWithOffset
}

func NewInMemoryMessageLog() MessageLog {
	return &inMemoryMessageLog{
		lock:     sync.RWMutex{},
		messages: nil}
}

func (log *inMemoryMessageLog) Append(message *pb.Message) WriteReceipt {
	receipt := newReceipt()

	log.lock.Lock()
	defer log.lock.Unlock()

	offset := int64(len(log.messages))

	msgWithOffset := &pb.MessageWithOffset{Message: message, Offset: offset}
	log.messages = append(log.messages, msgWithOffset)
	receipt.succeed(offset)

	return receipt
}

func (log *inMemoryMessageLog) CursorStart() (MessageLogCursor, error) {
	return &inMemoryMessageLogCursor{0, log}, nil
}

func (log *inMemoryMessageLog) CursorEnd() (MessageLogCursor, error) {
	lastOffset, err := log.LastOffset()
	if err != nil {
		switch err.(type) {
		case *EmptyLogError:
			return log.CursorStart()
		default:
			return nil, err
		}
	}

	return &inMemoryMessageLogCursor{lastOffset + 1, log}, nil
}

func (log *inMemoryMessageLog) CursorAt(offset int64) (MessageLogCursor, error) {
	lastOffset, err := log.LastOffset()
	if err != nil {
		return nil, err
	} else if offset > lastOffset {
		return nil, &OffsetOutOfBoundsError{offset, lastOffset}
	}

	return &inMemoryMessageLogCursor{offset, log}, nil
}

func (log *inMemoryMessageLog) FirstOffset() (int64, error) {
	log.lock.RLock()
	defer log.lock.RUnlock()

	if len(log.messages) == 0 {
		return -1, &EmptyLogError{}
	} else {
		return 0, nil
	}
}

func (log *inMemoryMessageLog) LastOffset() (int64, error) {
	log.lock.RLock()
	defer log.lock.RUnlock()

	if len(log.messages) == 0 {
		return -1, &EmptyLogError{}
	} else {
		return int64(len(log.messages) - 1), nil
	}
}

func (log *inMemoryMessageLog) read(pos int64) (*pb.MessageWithOffset, error) {
	log.lock.RLock()
	defer log.lock.RUnlock()

	if pos < 0 || pos >= int64(len(log.messages)) {
		return nil, &OffsetOutOfBoundsError{
			pos, int64(len(log.messages) - 1)}
	}

	return log.messages[int(pos)], nil
}

type inMemoryMessageLogCursor struct {
	pos int64
	log *inMemoryMessageLog
}

func (cursor *inMemoryMessageLogCursor) HasNext() bool {
	lastOffset, err := cursor.log.LastOffset()
	return err == nil && cursor.pos <= lastOffset
}

func (cursor *inMemoryMessageLogCursor) Next() (*pb.MessageWithOffset, error) {
	msg, error := cursor.log.read(cursor.pos)
	if error != nil {
		return nil, error
	}

	cursor.pos++

	return msg, nil
}

func (cursor *inMemoryMessageLogCursor) Pos() int64 {
	return cursor.pos
}

func (cursor *inMemoryMessageLogCursor) Seek(offset int64) error {
	lastOffset, err := cursor.log.LastOffset()
	if err != nil {
		return err
	} else if offset < 0 || offset > lastOffset {
		return &OffsetOutOfBoundsError{offset, lastOffset}
	}

	cursor.pos = offset
	return nil
}

type receiptImpl struct {
	offset   int64
	err      error
	resolved bool
	done     chan interface{}
}

func newReceipt() *receiptImpl {
	return &receiptImpl{-1, nil, false, make(chan interface{})}
}

func (r *receiptImpl) Done() chan interface{} {
	return r.done
}

func (r *receiptImpl) Read() (int64, error) {
	if !r.resolved {
		return -1, &ReceiptNotWrittenError{}
	}

	return r.offset, r.err
}

func (r *receiptImpl) resolve(offset int64, err error) {
	r.resolved = true
	r.offset = offset
	r.err = err
	close(r.done)
}

func (r *receiptImpl) succeed(offset int64) {
	r.resolve(offset, nil)
}

func (r *receiptImpl) fail(err error) {
	r.resolve(-1, err)
}
