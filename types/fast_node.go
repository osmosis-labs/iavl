package types

import (
	"io"

	"github.com/cosmos/iavl/cache"
	"github.com/cosmos/iavl/utils"
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
	ver, n, cause := utils.DecodeVarint(buf)
	if cause != nil {
		return nil, errors.Wrap(cause, "decoding fastnode.version")
	}
	buf = buf[n:]

	val, _, cause := utils.DecodeBytes(buf)
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

func (fn *FastNode) GetKey() []byte {
	return fn.key
}

func (fn *FastNode) GetValue() []byte {
	return fn.value
}

func (fn *FastNode) GetVersionLastUpdatedAt() int64 {
	return fn.versionLastUpdatedAt
}

func (fn *FastNode) EncodedSize() int {
	n := utils.EncodeVarintSize(fn.versionLastUpdatedAt) + utils.EncodeBytesSize(fn.value)
	return n
}

// writeBytes writes the FastNode as a serialized byte slice to the supplied io.Writer.
func (fn *FastNode) WriteBytes(w io.Writer) error {
	if fn == nil {
		return errors.New("cannot write nil node")
	}
	cause := utils.EncodeVarint(w, fn.versionLastUpdatedAt)
	if cause != nil {
		return errors.Wrap(cause, "writing version last updated at")
	}
	cause = utils.EncodeBytes(w, fn.value)
	if cause != nil {
		return errors.Wrap(cause, "writing value")
	}
	return nil
}
