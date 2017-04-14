package core

import (
	"sync"

	"github.com/emef/ultrabus/pb"
)

type MessageLogCursor interface {
	// True if the cursor has a next offset to traverse to
	HasNext() bool

	// Returns the next message and moves cursor forward in the log
	Next() (*pb.Message, error)

	// Returns the current offset of the cursor
	Pos() int64

	// Seek to the given offset
	Seek(offset int64) error
}

type MessageLog interface {
	// Append the message to the log, returning the offset of this message
	Append(message *pb.Message) (int64, error)

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

type inMemoryMessageLog struct {
	lock     sync.RWMutex
	messages []*pb.Message
}

func NewInMemoryMessageLog() MessageLog {
	return &inMemoryMessageLog{
		lock:     sync.RWMutex{},
		messages: nil}
}

func (log *inMemoryMessageLog) Append(message *pb.Message) (int64, error) {
	log.lock.Lock()
	defer log.lock.Unlock()

	offset := int64(len(log.messages))
	log.messages = append(log.messages, message)

	return offset, nil
}

func (log *inMemoryMessageLog) CursorStart() (MessageLogCursor, error) {
	return log.CursorAt(0)
}

func (log *inMemoryMessageLog) CursorEnd() (MessageLogCursor, error) {
	lastOffset, err := log.LastOffset()
	if err != nil {
		return nil, err
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

func (log *inMemoryMessageLog) read(pos int64) (*pb.Message, error) {
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

func (cursor *inMemoryMessageLogCursor) Next() (*pb.Message, error) {
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
