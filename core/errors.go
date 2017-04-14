package core

import (
	"fmt"
)

type OffsetOutOfBoundsError struct {
	badOffset, maxOffset int64
}

func (e *OffsetOutOfBoundsError) Error() string {
	return fmt.Sprintf("Offset %v is out of bounds (max offset %v)",
		e.badOffset, e.maxOffset)
}

type EmptyLogError struct{}

func (e *EmptyLogError) Error() string { return "Empty log" }
