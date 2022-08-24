package iavl

import (
	"io"

	"github.com/cosmos/iavl/cache"
	"github.com/cosmos/iavl/common"
	"github.com/pkg/errors"
)

// NOTE: This file favors int64 as opposed to int for size/counts.
// The Tree on the other hand favors int.  This is intentional.

type FastNode struct {
	key                  []byte
	versionLastUpdatedAt int64
	value                []byte
}

var _ cache.Node = (*FastNode)(nil)

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

// GetKey returns a node's key
// Implements cache.Node interface.
func (fn *FastNode) GetKey() []byte {
	return fn.key
}

// GetFullSize returns the number of bytes a node occupies in memory.
// Implements cache.Node interface. It is needed to enable cache's bytes limit decorator.
//
// Here, we estimate the following:
// key                  []byte - number of bytes in the slice + the underlying slice's structure
// versionLastUpdatedAt int64 - size of the 64 bit integer
// value                []byte - number of bytes in the slice + the underlying slice's structure
func (fn *FastNode) GetFullSize() int {
	return len(fn.key) + common.GetSliceSizeBytes() +
		common.Uint64Size +
		len(fn.value) + common.GetSliceSizeBytes()
}

func (node *FastNode) encodedSize() int {
	n := encodeVarintSize(node.versionLastUpdatedAt) + encodeBytesSize(node.value)
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
