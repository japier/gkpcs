package gkpcs

import (
	"encoding/binary"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/desc"
	"strings"
)

const MagicByte = 0

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

func byteArrayToIndexesSize(bytes []byte) (int, error) {
	count, n := binary.Varint(bytes)
	var size int
	if n <= 0 {
		return 0, fmt.Errorf("Error decoding indexes: invalid size")
	}

	size += n
	for i := 0; i < int(count); i++ {
		_, n := binary.Varint(bytes[size:])

		if n <= 0 {
			return 0, fmt.Errorf("Error decoding indexes: invalid size")
		}
		size += n
	}
	return size, nil
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
		return nil, fmt.Errorf("Error loading message descriptor: %w", err)
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

func Deserialize(data []byte, msg proto.Message) (int, error) {

	// Check message includes at least the magic byte and the schema id
	if len(data) < 5 {
		return 0, fmt.Errorf("Error on deserialization: invalid data to parse")
	}

	// Check Migic Byte
	if data[0] != MagicByte {
		return 0, fmt.Errorf("Error on deserialization: invalid magic byte '%d, must be '%d'", data[0], MagicByte)
	}
	data = data[1:]

	// Get schemaID
	schemaID := int(binary.BigEndian.Uint32(data[:4]))
	data = data[4:]

	// Get and parse message
	var (
		idxBytes int
		err      error
	)

	// Get index size
	if idxBytes, err = byteArrayToIndexesSize(data); err != nil {
		return 0, fmt.Errorf("Error on deserialization: parsing indexes %w", err)
	}

	data = data[idxBytes:]

	if err := proto.Unmarshal(data, msg); err != nil {
		return 0, fmt.Errorf("Error on deserialization: %w", err)
	}

	return schemaID, nil
}
