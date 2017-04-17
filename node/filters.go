package node

import (
	"github.com/emef/ultrabus/pb"
)

type MessageFilter interface {
	Applies(message *pb.MessageWithOffset) bool
}

type YesFilter struct{}

func (f *YesFilter) Applies(_ *pb.MessageWithOffset) bool { return true }
