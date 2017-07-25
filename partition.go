package ultrabus

import (
	"sync"

	"github.com/emef/ultrabus/pb"
)

type ConnectionHandle struct {
	partition *Partition
	clientID  *pb.ClientID
	stream    pb.UltrabusNode_SubscribeServer
	cursor    MessageLogCursor
	filter    MessageFilter
	notify    chan interface{}
	done      chan error
}

type Partition struct {
	lock        sync.RWMutex
	log         MessageLog
	connections map[pb.ClientID]*ConnectionHandle
	notify      chan interface{}
	done        chan interface{}
}

func NewInMemoryPartition() *Partition {
	partition := &Partition{
		sync.RWMutex{},
		NewInMemoryMessageLog(),
		make(map[pb.ClientID]*ConnectionHandle),
		make(chan interface{}, 1),
		make(chan interface{}, 1)}

	go partition.loop()

	return partition
}

func (partition *Partition) Stop() {
	partition.lock.Lock()
	defer partition.lock.Unlock()

	for clientID := range partition.connections {
		partition.unregisterConsumer(&clientID, &PartitionStoppedError{})
	}

	close(partition.done)
	close(partition.notify)
}

func (partition *Partition) Append(msg *pb.Message) (int64, error) {
	receipt := partition.log.Append(msg)
	<-receipt.Done()

	// non-blocking notify
	select {
	case partition.notify <- nil:
	default:
	}

	return receipt.Read()
}

func (partition *Partition) unregisterConsumer(
	clientID *pb.ClientID, err error) {

	partition.lock.Lock()
	defer partition.lock.Unlock()

	handle, ok := partition.connections[*clientID]
	if ok {
		delete(partition.connections, *clientID)
		handle.stop(err)
	}
}

func (partition *Partition) RegisterConsumer(
	clientID *pb.ClientID,
	stream pb.UltrabusNode_SubscribeServer) (*ConnectionHandle, error) {

	partition.lock.RLock()
	_, alreadyExists := partition.connections[*clientID]
	partition.lock.RUnlock()

	if alreadyExists {
		err := &DuplicateClientIDError{clientID}
		partition.unregisterConsumer(clientID, err)
	}

	cursor, err := partition.log.CursorEnd()
	if err != nil {
		return nil, err
	}

	handle := &ConnectionHandle{
		partition,
		clientID,
		stream,
		cursor,
		&YesFilter{},
		make(chan interface{}, 1),
		make(chan error, 1)}

	go handle.loop()

	partition.lock.Lock()
	partition.connections[*clientID] = handle
	partition.lock.Unlock()

	return handle, nil
}

func (partition *Partition) loop() {
	for {
		select {
		case <-partition.notify:
			partition.notifyAll()

		case <-partition.done:
			break
		}
	}
}

func (partition *Partition) notifyAll() {
	partition.lock.RLock()
	defer partition.lock.RUnlock()

	for _, handle := range partition.connections {
		// non-blocking notify
		select {
		case handle.notify <- nil:
		default:
		}
	}
}

func (handle *ConnectionHandle) Wait() error {
	return <-handle.done
}

func (handle *ConnectionHandle) stop(err error) {
	close(handle.notify)
	handle.done <- err
	close(handle.done)
}

func (handle *ConnectionHandle) loop() {
	for {
		select {
		case <-handle.notify:
			var messages []*pb.MessageWithOffset
			for handle.cursor.HasNext() {
				msgWithOffset, err := handle.cursor.Next()
				if err != nil {
					handle.partition.unregisterConsumer(handle.clientID, err)
					return
				}

				if handle.filter.Applies(msgWithOffset) {
					messages = append(messages, msgWithOffset)
				}
			}

			if len(messages) > 0 {
				resp := &pb.Messages{messages}
				if err := handle.stream.Send(resp); err != nil {
					handle.partition.unregisterConsumer(handle.clientID, err)
					return
				}
			}

		case <-handle.done:
			return
		}
	}
}
