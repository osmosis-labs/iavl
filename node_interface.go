package iavl

import "io"

type Node interface {
	Key() []byte
	Value() []byte
	SetKey(key []byte)
	Version() int64
	WriteBytes(w io.Writer) error
	EncodedSize() int
}

type ComplexNode interface {
	Node
	Hash() []byte
	Persisted() bool
	SetPersisted(persisted bool)
	Leaf() bool
	Height() int8
	LeftHash() []byte
	RightHash() []byte
	LeftNode() *TreeNode
	RightNode() *TreeNode
	SetLeftHash(hash []byte)
	SetRightHash(hash []byte)
	_hash() []byte
	setLeftNode(newLeftNode ComplexNode)
	setRightNode(newRightNode ComplexNode)
	Size() int64
	SetHash(hash []byte)
}
