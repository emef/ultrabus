package core

import (
	"testing"

	"github.com/emef/ultrabus/pb"
	"github.com/stretchr/testify/assert"
)

func TestInMemoryLog(t *testing.T) {
	log := NewInMemoryMessageLog()
	basicLogTests(t, log)
}

func basicLogTests(t *testing.T, log MessageLog) {
	assert := assert.New(t)

	_, err := log.LastOffset()
	assert.NotNil(err)

	msg1 := &pb.Message{Key: []byte("key_1"), Value: []byte("value_1")}
	offset1, err := log.Append(msg1)
	assert.Nil(err)
	assert.False(offset1 < 0)

	msg2 := &pb.Message{Key: []byte("key_1"), Value: []byte("value_1")}
	offset2, err := log.Append(msg2)
	assert.Nil(err)
	assert.False(offset2 < 0, "Offset is negative")
	assert.True(offset1 < offset2)

	firstOffset, err := log.FirstOffset()
	assert.Nil(err)
	assert.Equal(firstOffset, offset1)

	lastOffset, err := log.LastOffset()
	assert.Nil(err)
	assert.Equal(lastOffset, offset2)

	cursor, err := log.CursorAt(offset1)
	assert.Nil(err)
	assert.NotNil(cursor)
	assert.True(cursor.HasNext())
	assert.Equal(cursor.Pos(), offset1)

	// Get first message
	msg, err := cursor.Next()
	assert.Nil(err)
	assert.Equal(msg.Message, msg1)
	assert.Equal(msg.Offset, offset1)
	assert.True(cursor.HasNext())
	assert.Equal(cursor.Pos(), offset2)

	// Get second message
	msg, err = cursor.Next()
	assert.Nil(err)
	assert.Equal(msg.Message, msg2)
	assert.Equal(msg.Offset, offset2)
	assert.False(cursor.HasNext())

	// We are out of bounds, expect error
	msg, err = cursor.Next()
	assert.Nil(msg)
	assert.NotNil(err)

	// Seek back to second message
	err = cursor.Seek(offset2)
	assert.Nil(err)
	msg, err = cursor.Next()
	assert.NotNil(msg)
	assert.Nil(err)
	assert.Equal(msg.Message, msg2)
	assert.Equal(msg.Offset, offset2)

	// Seek to invalid locations
	err = cursor.Seek(-1)
	assert.NotNil(err)

	err = cursor.Seek(offset2 + 1)
	assert.NotNil(err)

	// Cursor at beginning of the log
	cursor, err = log.CursorStart()
	assert.Nil(err)
	assert.NotNil(cursor)
	assert.Equal(cursor.Pos(), offset1)
	assert.True(cursor.HasNext())

	msg, err = cursor.Next()
	assert.Nil(err)
	assert.Equal(msg.Message, msg1)
	assert.Equal(msg.Offset, offset1)

	// Cursor at the end of the log
	cursor, err = log.CursorEnd()
	assert.Nil(err)
	assert.NotNil(cursor)
	assert.False(cursor.HasNext())

	// Expect error because we are at the end of the cursor
	msg, err = cursor.Next()
	assert.Nil(msg)
	assert.NotNil(err)
}
