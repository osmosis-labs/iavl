package iavl

import (
	"github.com/pkg/errors"
	"io"
)

// NOTE: This file favors int64 as opposed to int for size/counts.
// The Tree on the other hand favors int.  This is intentional.

type Node interface {
	GetKey() []byte
	GetValue() []byte
	SetKey(key []byte)
	GetVersion() int64
	WriteBytes(w io.Writer) error
	EncodedSize() int

}
type FastNode struct {
	key                  []byte
	versionLastUpdatedAt int64
	value                []byte
}

func (node *FastNode) GetValue() []byte{
	return node.value
}

func (node *FastNode) GetKey() []byte{
	return node.key
}

func (node *FastNode) SetKey(key []byte){
	node.key = key
}

func (node *FastNode) GetVersion() int64{
	return node.versionLastUpdatedAt
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
func DeserializeFastNode(key []byte, buf []byte) (Node, error) {
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

func (node *FastNode) EncodedSize() int {
	n := encodeVarintSize(node.versionLastUpdatedAt) + encodeBytesSize(node.value)
	return n
}

// writeBytes writes the FastNode as a serialized byte slice to the supplied io.Writer.
func (node *FastNode) WriteBytes(w io.Writer) error {
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
