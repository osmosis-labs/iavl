package iavl

// NOTE: This file favors int64 as opposed to int for size/counts.
// The Tree on the other hand favors int.  This is intentional.

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"math"

	"github.com/pkg/errors"
)

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

// TreeNode represents a node in a Tree.
type TreeNode struct {
	key       []byte
	value     []byte
	hash      []byte
	leftHash  []byte
	rightHash []byte
	version   int64
	size      int64
	leftNode  *TreeNode
	rightNode *TreeNode
	height    int8
	persisted bool
}

func (node *TreeNode) SetHash(hash []byte) {
	node.hash = hash
}
func (node *TreeNode) Size() int64 {
	if node == nil {
		panic("lol")
	}
	return node.size
}
func (node *TreeNode) setLeftNode(newLeftNode ComplexNode) {
	if newLeftNode == nil {
		node.leftNode = nil
		return
	}
	node.leftNode = newLeftNode.(*TreeNode)
}
func (node *TreeNode) setRightNode(newRightNode ComplexNode) {
	if newRightNode == nil {
		node.rightNode = nil
		return
	}
	node.rightNode = newRightNode.(*TreeNode)
}

func (node *TreeNode) SetLeftHash(hash []byte) {
	node.leftHash = hash
}
func (node *TreeNode) SetRightHash(hash []byte) {
	node.rightHash = hash
}

func (node *TreeNode) LeftNode() *TreeNode {
	return node.leftNode
}
func (node *TreeNode) RightNode() *TreeNode {
	return node.rightNode
}

func (node *TreeNode) LeftHash() []byte {
	return node.leftHash
}
func (node *TreeNode) RightHash() []byte {
	return node.rightHash
}

func (node *TreeNode) Height() int8 {
	return node.height
}

func (node *TreeNode) Hash() []byte {
	return node.hash
}
func (node *TreeNode) Persisted() bool {
	return node.persisted
}

func (node *TreeNode) Version() int64 {
	return node.version
}

func (node *TreeNode) Key() []byte {
	return node.key
}
func (node *TreeNode) Value() []byte {
	return node.value
}
func (node *TreeNode) SetKey(key []byte) {
	node.key = key
}

func (node *TreeNode) SetPersisted(persisted bool) {
	node.persisted = persisted
}

// NewNode returns a new node from a key, value and version.
func NewNode(key []byte, value []byte, version int64) *TreeNode {
	return &TreeNode{
		key:     key,
		value:   value,
		height:  0,
		size:    1,
		version: version,
	}
}

// MakeNode constructs an *TreeNode from an encoded byte slice.
//
// The new node doesn't have its hash saved or set. The caller must set it
// afterwards.
func MakeNode(buf []byte) (*TreeNode, error) {

	// Read node header (height, size, version, key).
	height, n, cause := decodeVarint(buf)
	if cause != nil {
		return nil, errors.Wrap(cause, "decoding node.height")
	}
	buf = buf[n:]
	if height < int64(math.MinInt8) || height > int64(math.MaxInt8) {
		return nil, errors.New("invalid height, must be int8")
	}

	size, n, cause := decodeVarint(buf)
	if cause != nil {
		return nil, errors.Wrap(cause, "decoding node.size")
	}
	buf = buf[n:]

	ver, n, cause := decodeVarint(buf)
	if cause != nil {
		return nil, errors.Wrap(cause, "decoding node.version")
	}
	buf = buf[n:]

	key, n, cause := decodeBytes(buf)
	if cause != nil {
		return nil, errors.Wrap(cause, "decoding node.key")
	}
	buf = buf[n:]

	node := &TreeNode{
		height:  int8(height),
		size:    size,
		version: ver,
		key:     key,
	}

	// Read node body.

	if node.Leaf() {
		val, _, cause := decodeBytes(buf)
		if cause != nil {
			return nil, errors.Wrap(cause, "decoding node.value")
		}
		node.value = val
	} else { // Read children.
		leftHash, n, cause := decodeBytes(buf)
		if cause != nil {
			return nil, errors.Wrap(cause, "deocding node.leftHash")
		}
		buf = buf[n:]

		rightHash, _, cause := decodeBytes(buf)
		if cause != nil {
			return nil, errors.Wrap(cause, "decoding node.rightHash")
		}
		node.leftHash = leftHash
		node.rightHash = rightHash
	}
	return node, nil
}

// String returns a string representation of the node.
func (node *TreeNode) String() string {
	hashstr := "<no hash>"
	if len(node.hash) > 0 {
		hashstr = fmt.Sprintf("%X", node.hash)
	}
	return fmt.Sprintf("TreeNode{%s:%s@%d %X;%X}#%s",
		ColoredBytes(node.key, Green, Blue),
		ColoredBytes(node.value, Cyan, Blue),
		node.version,
		node.leftHash, node.rightHash,
		hashstr)
}

// clone creates a shallow copy of a node with its hash set to nil.
func (node *TreeNode) clone(version int64) *TreeNode {
	if node.Leaf() {
		panic("Attempt to copy a leaf node")
	}
	return &TreeNode{
		key:       node.key,
		height:    node.height,
		version:   version,
		size:      node.size,
		hash:      nil,
		leftHash:  node.leftHash,
		leftNode:  node.leftNode,
		rightHash: node.rightHash,
		rightNode: node.rightNode,
		persisted: false,
	}
}

func (node *TreeNode) Leaf() bool {
	return node.height == 0
}

// Check if the node has a descendant with the given key.
func (node *TreeNode) has(t *ImmutableTree, key []byte) (has bool) {
	if bytes.Equal(node.key, key) {
		return true
	}
	if node.Leaf() {
		return false
	}
	if bytes.Compare(key, node.key) < 0 {
		return node.getLeftNodeFromTree(t).has(t, key)
	}
	return node.getRightNodeFromTree(t).has(t, key)
}

// Get a key under the node.
//
// The index is the index in the list of leaf nodes sorted lexicographically by key. The leftmost leaf has index 0.
// It's neighbor has index 1 and so on.
func (node *TreeNode) get(t *ImmutableTree, key []byte) (index int64, value []byte) {
	if node.Leaf() {
		switch bytes.Compare(node.key, key) {
		case -1:
			return 1, nil
		case 1:
			return 0, nil
		default:
			return 0, node.value
		}
	}

	if bytes.Compare(key, node.key) < 0 {
		return node.getLeftNodeFromTree(t).get(t, key)
	}
	rightNode := node.getRightNodeFromTree(t)
	index, value = rightNode.get(t, key)
	index += node.size - rightNode.Size()
	return index, value
}

func (node *TreeNode) getByIndex(t *ImmutableTree, index int64) (key []byte, value []byte) {
	if node.Leaf() {
		if index == 0 {
			return node.key, node.value
		}
		return nil, nil
	}
	// TODO: could improve this by storing the
	// sizes as well as left/right hash.
	leftNode := node.getLeftNodeFromTree(t)

	if index < leftNode.Size() {
		return leftNode.getByIndex(t, index)
	}
	return node.getRightNodeFromTree(t).getByIndex(t, index-leftNode.Size())
}

// Computes the hash of the node without computing its descendants. Must be
// called on nodes which have descendant node hashes already computed.
func (node *TreeNode) _hash() []byte {
	if node.hash != nil {
		return node.hash
	}

	h := sha256.New()
	buf := new(bytes.Buffer)
	if err := node.writeHashBytes(buf); err != nil {
		panic(err)
	}
	_, err := h.Write(buf.Bytes())
	if err != nil {
		panic(err)
	}
	node.hash = h.Sum(nil)

	return node.hash
}

// Hash the node and its descendants recursively. This usually mutates all
// descendant nodes. Returns the node hash and number of nodes hashed.
// If the tree is empty (i.e. the node is nil), returns the hash of an empty input,
// to conform with RFC-6962.
func (node *TreeNode) hashWithCount() ([]byte, int64) {
	if node == nil {
		return sha256.New().Sum(nil), 0
	}
	if node.hash != nil {
		return node.hash, 0
	}

	h := sha256.New()
	buf := new(bytes.Buffer)
	hashCount, err := node.writeHashBytesRecursively(buf)
	if err != nil {
		panic(err)
	}
	_, err = h.Write(buf.Bytes())
	if err != nil {
		panic(err)
	}
	node.hash = h.Sum(nil)

	return node.hash, hashCount + 1
}

// validate validates the node contents
func (node *TreeNode) validate() error {
	if node == nil {
		return errors.New("node cannot be nil")
	}
	if node.key == nil {
		return errors.New("key cannot be nil")
	}
	if node.version <= 0 {
		return errors.New("version must be greater than 0")
	}
	if node.height < 0 {
		return errors.New("height cannot be less than 0")
	}
	if node.size < 1 {
		return errors.New("size must be at least 1")
	}

	if node.height == 0 {
		// Leaf nodes
		if node.value == nil {
			return errors.New("value cannot be nil for leaf node")
		}
		if node.leftHash != nil || node.leftNode != nil || node.rightHash != nil || node.rightNode != nil {
			return errors.New("leaf node cannot have children")
		}
		if node.size != 1 {
			return errors.New("leaf nodes must have size 1")
		}
	} else {
		// Inner nodes
		if node.value != nil {
			return errors.New("value must be nil for non-leaf node")
		}
		if node.leftHash == nil && node.rightHash == nil {
			return errors.New("inner node must have children")
		}
	}
	return nil
}

// Writes the node's hash to the given io.Writer. This function expects
// child hashes to be already set.
func (node *TreeNode) writeHashBytes(w io.Writer) error {
	err := encodeVarint(w, int64(node.height))
	if err != nil {
		return errors.Wrap(err, "writing height")
	}
	err = encodeVarint(w, node.size)
	if err != nil {
		return errors.Wrap(err, "writing size")
	}
	err = encodeVarint(w, node.version)
	if err != nil {
		return errors.Wrap(err, "writing version")
	}

	// Key is not written for inner nodes, unlike WriteBytes.

	if node.Leaf() {
		err = encodeBytes(w, node.key)
		if err != nil {
			return errors.Wrap(err, "writing key")
		}

		// Indirection needed to provide proofs without values.
		// (e.g. ProofLeafNode.ValueHash)
		valueHash := sha256.Sum256(node.value)

		err = encodeBytes(w, valueHash[:])
		if err != nil {
			return errors.Wrap(err, "writing value")
		}
	} else {
		if node.leftHash == nil || node.rightHash == nil {
			panic("Found an empty child hash")
		}
		err = encodeBytes(w, node.leftHash)
		if err != nil {
			return errors.Wrap(err, "writing left hash")
		}
		err = encodeBytes(w, node.rightHash)
		if err != nil {
			return errors.Wrap(err, "writing right hash")
		}
	}

	return nil
}

// Writes the node's hash to the given io.Writer.
// This function has the side-effect of calling hashWithCount.
func (node *TreeNode) writeHashBytesRecursively(w io.Writer) (hashCount int64, err error) {
	if node.leftNode != nil {
		leftHash, leftCount := node.leftNode.hashWithCount()
		node.leftHash = leftHash
		hashCount += leftCount
	}
	if node.rightNode != nil {
		rightHash, rightCount := node.rightNode.hashWithCount()
		node.rightHash = rightHash
		hashCount += rightCount
	}
	err = node.writeHashBytes(w)

	return
}

func (node *TreeNode) EncodedSize() int {
	n := 1 +
		encodeVarintSize(node.size) +
		encodeVarintSize(node.version) +
		encodeBytesSize(node.key)
	if node.Leaf() {
		n += encodeBytesSize(node.value)
	} else {
		n += encodeBytesSize(node.leftHash) +
			encodeBytesSize(node.rightHash)
	}
	return n
}

// Writes the node as a serialized byte slice to the supplied io.Writer.
func (node *TreeNode) WriteBytes(w io.Writer) error {
	if node == nil {
		return errors.New("cannot write nil node")
	}
	cause := encodeVarint(w, int64(node.height))
	if cause != nil {
		return errors.Wrap(cause, "writing height")
	}
	cause = encodeVarint(w, node.size)
	if cause != nil {
		return errors.Wrap(cause, "writing size")
	}
	cause = encodeVarint(w, node.version)
	if cause != nil {
		return errors.Wrap(cause, "writing version")
	}

	// Unlike writeHashBytes, key is written for inner nodes.
	cause = encodeBytes(w, node.key)
	if cause != nil {
		return errors.Wrap(cause, "writing key")
	}

	if node.Leaf() {
		cause = encodeBytes(w, node.value)
		if cause != nil {
			return errors.Wrap(cause, "writing value")
		}
	} else {
		if node.leftHash == nil {
			panic("node.leftHash was nil in WriteBytes")
		}
		cause = encodeBytes(w, node.leftHash)
		if cause != nil {
			return errors.Wrap(cause, "writing left hash")
		}

		if node.rightHash == nil {
			panic("node.rightHash was nil in WriteBytes")
		}
		cause = encodeBytes(w, node.rightHash)
		if cause != nil {
			return errors.Wrap(cause, "writing right hash")
		}
	}
	return nil
}

func (node *TreeNode) getLeftNodeFromTree(t *ImmutableTree) *TreeNode {
	if node.leftNode != nil {
		return node.leftNode
	}
	return t.ndb.GetNode(node.leftHash)
}

func (node *TreeNode) getRightNodeFromTree(t *ImmutableTree) *TreeNode {
	if node.rightNode != nil {
		return node.rightNode
	}
	return t.ndb.GetNode(node.rightHash)
}

// NOTE: mutates height and size
func (node *TreeNode) calcHeightAndSize(t *ImmutableTree) {
	node.height = maxInt8(node.getLeftNodeFromTree(t).Height(), node.getRightNodeFromTree(t).Height()) + 1
	node.size = node.getLeftNodeFromTree(t).Size() + node.getRightNodeFromTree(t).Size()
}

func (node *TreeNode) calcBalance(t *ImmutableTree) int {
	return int(node.getLeftNodeFromTree(t).Height()) - int(node.getRightNodeFromTree(t).Height())
}

// traverse is a wrapper over traverseInRange when we want the whole tree
func (node *TreeNode) traverse(t *ImmutableTree, ascending bool, cb func(node ComplexNode) bool) bool {
	return node.traverseInRange(t, nil, nil, ascending, false, false, func(node ComplexNode) bool {
		return cb(node)
	})
}

// traversePost is a wrapper over traverseInRange when we want the whole tree post-order
func (node *TreeNode) TraversePost(t *ImmutableTree, ascending bool, cb func(node ComplexNode) bool) bool {
	return node.traverseInRange(t, nil, nil, ascending, false, true, func(node ComplexNode) bool {
		return cb(node)
	})
}

func (node *TreeNode) traverseInRange(tree *ImmutableTree, start, end []byte, ascending bool, inclusive bool, post bool, cb func(node ComplexNode) bool) bool {
	stop := false
	t := node.newTraversal(tree, start, end, ascending, inclusive, post)
	for node2 := t.next(); node2 != nil; node2 = t.next() {
		stop = cb(node2)
		if stop {
			return stop
		}
	}
	return stop
}

// Only used in testing...
func (node *TreeNode) lmd(t *ImmutableTree) *TreeNode {
	if node.Leaf() {
		return node
	}
	return node.getLeftNodeFromTree(t).lmd(t)
}
