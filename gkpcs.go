package gkpcs

import (
	"encoding/binary"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/desc"
	"strings"
)

func msgIndexesToByteArray(indexes []int) []byte {
	encoded := make([]byte, binary.MaxVarintLen32)
	result := make([]byte, 0)

	n := binary.PutVarint(encoded, int64(len(indexes)))
	result = append(result, encoded[:n]...)
	for _, index := range indexes {
		n = binary.PutVarint(encoded, int64(index))
		result = append(result, encoded[:n]...)
	}

	return result
}

func getMessageIndexes(desc *desc.FileDescriptor, name string) []int {
	indexes := []int{}
	parts := strings.Split(name, ".")

	messageTypes := desc.GetMessageTypes()
	for _, part := range parts {
		for i, mt := range messageTypes {
			if mt.GetName() == part {
				indexes = append(indexes, i)
				messageTypes = mt.GetNestedMessageTypes()
				break
			}
		}
	}

	return indexes
}

// Serialize recibes a protobuf message and serialized based on the confluent specification for kafka
// https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html
func Serialize(msg proto.Message) ([]byte, error) {
	value, _ := proto.Marshal(msg)

	var recordValue []byte
	schemaIDBytes := make([]byte, 4)

	binary.BigEndian.PutUint32(schemaIDBytes, uint32(1))

	msgDesc, err := desc.LoadMessageDescriptorForMessage(msg)
	if err != nil {
		fmt.Errorf("Error loading message descriptor: %w", err)
		return nil, err
	}

	fileDesc := msgDesc.GetFile()
	indexes := getMessageIndexes(fileDesc, msgDesc.GetFullyQualifiedName())

	msgIdxBytes := msgIndexesToByteArray(indexes)

	recordValue = append(recordValue, byte(0))
	recordValue = append(recordValue, schemaIDBytes...)
	recordValue = append(recordValue, msgIdxBytes...)
	recordValue = append(recordValue, value...)
	return recordValue, nil
}
