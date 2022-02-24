package iavl

import (
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
)

// NOTE: This file favors int64 as opposed to int for size/counts.
// The Tree on the other hand favors int.  This is intentional.

type FastNode struct {
	key                  []byte
	versionLastUpdatedAt int64
	value                []byte
}

// NewFastNode returns a new fast node from a value and version.
func NewFastNode(key []byte, value []byte, version int64) *FastNode {
	return &FastNode{
		key:                  key,
		versionLastUpdatedAt: version,
		value:                value,
	}
}

// DeserializeFastNode constructs an *FastNode from an encoded byte slice.
func DeserializeFastNode(key []byte, buf []byte) (*FastNode, error) {
	ver, n, cause := decodeVarint(buf)
	if cause != nil {
		return nil, errors.Wrap(cause, "decoding fastnode.version")
	}
	buf = buf[n:]

	val, _, cause := decodeBytes(buf)
	if cause != nil {
		return nil, errors.Wrap(cause, "decoding fastnode.value")
	}

	fastNode := &FastNode{
		key:                  key,
		versionLastUpdatedAt: ver,
		value:                val,
	}

	return fastNode, nil
}

func (node *FastNode) encodedValueSize() int {
	n := encodeVarintSize(node.versionLastUpdatedAt) + encodeBytesSize(node.value)
	return n
}

func (node *FastNode) encodedFullSize() int {
	// See: https://github.com/syndtr/goleveldb/blob/2ae1ddf74ef7020251ff1ff0fe8daac21a157761/leveldb/batch.go#L89
	n := 1 + binary.MaxVarintLen32 + len(node.key) + node.encodedValueSize()
	return n
}

// writeBytes writes the FastNode as a serialized byte slice to the supplied io.Writer.
func (node *FastNode) writeBytes(w io.Writer) error {
	if node == nil {
		return errors.New("cannot write nil node")
	}
	cause := encodeVarint(w, node.versionLastUpdatedAt)
	if cause != nil {
		return errors.Wrap(cause, "writing version last updated at")
	}
	cause = encodeBytes(w, node.value)
	if cause != nil {
		return errors.Wrap(cause, "writing value")
	}
	return nil
}
