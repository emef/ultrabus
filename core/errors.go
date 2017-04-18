package core

import (
	"fmt"

	"github.com/emef/ultrabus/pb"
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

type DuplicateClientIDError struct {
	ClientID *pb.ClientID
}

func (e *DuplicateClientIDError) Error() string {
	return fmt.Sprintf("Duplicate ClientID %v", e.ClientID)
}

type PartitionStoppedError struct{}

func (e *PartitionStoppedError) Error() string { return "Partition stopped" }

type PartitionNotFoundError struct {
	PartitionID *pb.PartitionID
}

func (e *PartitionNotFoundError) Error() string {
	return fmt.Sprintf("Partition not found: %v", e.PartitionID)
}

type ReceiptNotWrittenError struct{}

func (e *ReceiptNotWrittenError) Error() string {
	return "Receipt has not yet been written"
}
