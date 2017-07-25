package ultrabus

import (
	"crypto/md5"
	"encoding/binary"
	"errors"

	"github.com/emef/ultrabus/pb"
)

func HashToPartition(message *pb.Message, partitions int32) (int32, error) {
	if len(message.Key) == 0 {
		return -1, errors.New("Cannot hash message with no key")
	}

	partition := int32(hashToRange(message.Key, 0, int64(partitions)-1))
	return partition, nil
}

func hashCode(source []byte) int64 {
	strMd5 := md5.Sum(source)
	hashed, _ := binary.Varint(strMd5[:])
	return hashed
}

func hashToRange(source []byte, minValue int64, maxValue int64) int64 {
	hash := hashCode(source) & 0x7FFFFFFF
	delta := hash % (1 + maxValue - minValue)
	return minValue + delta
}
