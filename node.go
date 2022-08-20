package iavl

// NOTE: This file favors int64 as opposed to int for size/counts.
// The Tree on the other hand favors int.  This is intentional.

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"math"

	"github.com/cosmos/iavl/cache"
	"github.com/cosmos/iavl/utils"
	"github.com/pkg/errors"
)

// Node represents a node in a Tree.
type Node struct {
	// TODO: Change key to path
	key       []byte
	value     []byte
	hash      []byte
	leftHash  []byte
	rightHash []byte
	// TODO: Add here leftVersion, rightVersion, and then we change child discovery in db to use that.
	version   int64
	size      int64
	leftNode  *Node
	rightNode *Node
	// TODO: Delete depth. should become derived off len(path)
	depth     int8
	persisted bool
}

var _ cache.Node = (*Node)(nil)

// NewNode returns a new node from a key, value and version.
func NewNode(key []byte, value []byte, version int64) *Node {
	return &Node{
		key:     key,
		value:   value,
		depth:   0,
		size:    1,
		version: version,
	}
}

// DeserializeNode constructs an *Node from a serialized byte slice.
//
// The new node doesn't have its hash saved or set, as in the current IAVL schema it is not serialized within the node.
// The caller must set it afterwards. (NOTE: It is easily derivable, via just running the hash of data in the node)
func DeserializeNode(buf []byte) (*Node, error) {
	// Read node header (depth, size, version, key).
	depth, n, cause := utils.DecodeVarint(buf)
	if cause != nil {
		return nil, errors.Wrap(cause, "decoding node.depth")
	}
	buf = buf[n:]
	if depth < int64(math.MinInt8) || depth > int64(math.MaxInt8) {
		return nil, errors.New("invalid depth, must be int8")
	}

	size, n, cause := utils.DecodeVarint(buf)
	if cause != nil {
		return nil, errors.Wrap(cause, "decoding node.size")
	}
	buf = buf[n:]

	ver, n, cause := utils.DecodeVarint(buf)
	if cause != nil {
		return nil, errors.Wrap(cause, "decoding node.version")
	}
	buf = buf[n:]

	key, n, cause := utils.DecodeBytes(buf)
	if cause != nil {
		return nil, errors.Wrap(cause, "decoding node.key")
	}
	buf = buf[n:]

	node := &Node{
		depth:   int8(depth),
		size:    size,
		version: ver,
		key:     key,
	}

	// Read node body.

	if node.isLeaf() {
		val, _, cause := utils.DecodeBytes(buf)
		if cause != nil {
			return nil, errors.Wrap(cause, "decoding node.value")
		}
		node.value = val
	} else { // Read children.
		leftHash, n, cause := utils.DecodeBytes(buf)
		if cause != nil {
			return nil, errors.Wrap(cause, "deocding node.leftHash")
		}
		buf = buf[n:]

		rightHash, _, cause := utils.DecodeBytes(buf)
		if cause != nil {
			return nil, errors.Wrap(cause, "decoding node.rightHash")
		}
		node.leftHash = leftHash
		node.rightHash = rightHash
	}
	return node, nil
}

func (n *Node) GetKey() []byte {
	return n.hash
}

// String returns a string representation of the node.
func (node *Node) String() string {
	hashstr := "<no hash>"
	if len(node.hash) > 0 {
		hashstr = fmt.Sprintf("%X", node.hash)
	}
	return fmt.Sprintf("Node{%s:%s@%d %X;%X}#%s",
		utils.ColoredBytes(node.key, utils.Green, utils.Blue),
		utils.ColoredBytes(node.value, utils.Cyan, utils.Blue),
		node.version,
		node.leftHash, node.rightHash,
		hashstr)
}

// clone creates a shallow copy of a node with its hash set to nil.
func (node *Node) clone(version int64) *Node {
	if node.isLeaf() {
		panic("Attempt to copy a leaf node")
	}
	return &Node{
		key:       node.key,
		depth:     node.depth,
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

func (node *Node) isLeaf() bool {
	return node.depth == 0
}

// Check if the node has a descendant with the given key.
func (node *Node) has(t *ImmutableTree, key []byte) (has bool) {
	if bytes.Equal(node.key, key) {
		return true
	}
	if node.isLeaf() {
		return false
	}
	if bytes.Compare(key, node.key) < 0 {
		return t.getLeftChild(node).has(t, key)
	}
	return t.getRightChild(node).has(t, key)
}

// Get a key under the node.
//
// The index is the index in the list of leaf nodes sorted lexicographically by key. The leftmost leaf has index 0.
// It's neighbor has index 1 and so on.
func (node *Node) get(t *ImmutableTree, key []byte) (index int64, value []byte) {
	if node.isLeaf() {
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
		return t.getLeftChild(node).get(t, key)
	}
	rightNode := t.getRightChild(node)
	index, value = rightNode.get(t, key)
	index += node.size - rightNode.size
	return index, value
}

// Computes the hash of the node without computing its descendants. Must be
// called on nodes which have descendant node hashes already computed.
func (node *Node) _hash() []byte {
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
func (node *Node) hashWithCount() ([]byte, int64) {
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
func (node *Node) validate() error {
	if node == nil {
		return errors.New("node cannot be nil")
	}
	if node.key == nil {
		return errors.New("key cannot be nil")
	}
	if node.version <= 0 {
		return errors.New("version must be greater than 0")
	}
	if node.depth < 0 {
		return errors.New("depth cannot be less than 0")
	}
	if node.size < 1 {
		return errors.New("size must be at least 1")
	}

	if node.depth == 0 {
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
// Note to future people, it just sucks we have to preserve this function,
// e.g. keeping depth size and version in here...
func (node *Node) writeHashBytes(w io.Writer) error {
	err := utils.EncodeVarint(w, int64(node.depth))
	if err != nil {
		return errors.Wrap(err, "writing depth")
	}
	err = utils.EncodeVarint(w, node.size)
	if err != nil {
		return errors.Wrap(err, "writing size")
	}
	err = utils.EncodeVarint(w, node.version)
	if err != nil {
		return errors.Wrap(err, "writing version")
	}

	// Key is not written for inner nodes, unlike writeBytes.

	if node.isLeaf() {
		err = utils.EncodeBytes(w, node.key)
		if err != nil {
			return errors.Wrap(err, "writing key")
		}

		// Indirection needed to provide proofs without values.
		// (e.g. ProofLeafNode.ValueHash)
		valueHash := sha256.Sum256(node.value)

		err = utils.EncodeBytes(w, valueHash[:])
		if err != nil {
			return errors.Wrap(err, "writing value")
		}
	} else {
		if node.leftHash == nil || node.rightHash == nil {
			panic("Found an empty child hash")
		}
		err = utils.EncodeBytes(w, node.leftHash)
		if err != nil {
			return errors.Wrap(err, "writing left hash")
		}
		err = utils.EncodeBytes(w, node.rightHash)
		if err != nil {
			return errors.Wrap(err, "writing right hash")
		}
	}

	return nil
}

// Writes the node's hash to the given io.Writer.
// This function has the side-effect of calling hashWithCount.
func (node *Node) writeHashBytesRecursively(w io.Writer) (hashCount int64, err error) {
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

func (node *Node) encodedSize() int {
	n := 1 +
		utils.EncodeVarintSize(node.size) +
		utils.EncodeVarintSize(node.version) +
		utils.EncodeBytesSize(node.key)
	if node.isLeaf() {
		n += utils.EncodeBytesSize(node.value)
	} else {
		n += utils.EncodeBytesSize(node.leftHash) +
			utils.EncodeBytesSize(node.rightHash)
	}
	return n
}

// Writes the node as a serialized byte slice to the supplied io.Writer.
func (node *Node) writeBytes(w io.Writer) error {
	if node == nil {
		return errors.New("cannot write nil node")
	}
	cause := utils.EncodeVarint(w, int64(node.depth))
	if cause != nil {
		return errors.Wrap(cause, "writing depth")
	}
	cause = utils.EncodeVarint(w, node.size)
	if cause != nil {
		return errors.Wrap(cause, "writing size")
	}
	cause = utils.EncodeVarint(w, node.version)
	if cause != nil {
		return errors.Wrap(cause, "writing version")
	}

	// Unlike writeHashBytes, key is written for inner nodes.
	cause = utils.EncodeBytes(w, node.key)
	if cause != nil {
		return errors.Wrap(cause, "writing key")
	}

	if node.isLeaf() {
		cause = utils.EncodeBytes(w, node.value)
		if cause != nil {
			return errors.Wrap(cause, "writing value")
		}
	} else {
		if node.leftHash == nil {
			panic("node.leftHash was nil in writeBytes")
		}
		cause = utils.EncodeBytes(w, node.leftHash)
		if cause != nil {
			return errors.Wrap(cause, "writing left hash")
		}

		if node.rightHash == nil {
			panic("node.rightHash was nil in writeBytes")
		}
		cause = utils.EncodeBytes(w, node.rightHash)
		if cause != nil {
			return errors.Wrap(cause, "writing right hash")
		}
	}
	return nil
}

func (node *Node) getByIndex(t *ImmutableTree, index int64) (key []byte, value []byte) {
	if node.isLeaf() {
		if index == 0 {
			return node.key, node.value
		}
		return nil, nil
	}
	// TODO: could improve this by storing the
	// sizes as well as left/right hash.
	leftNode := t.getLeftChild(node)

	if index < leftNode.size {
		return leftNode.getByIndex(t, index)
	}
	return t.getRightChild(node).getByIndex(t, index-leftNode.size)
}

// NOTE: mutates depth and size
func (node *Node) calcHeightAndSize(t *ImmutableTree) {
	node.depth = maxInt8(t.getLeftChild(node).depth, t.getRightChild(node).depth) + 1
	node.size = t.getLeftChild(node).size + t.getRightChild(node).size
}

func (node *Node) calcBalance(t *ImmutableTree) int {
	return int(t.getLeftChild(node).depth) - int(t.getRightChild(node).depth)
}

// traverse is a wrapper over traverseInRange when we want the whole tree
func (node *Node) traverse(t *ImmutableTree, ascending bool, cb func(*Node) bool) bool {
	return node.traverseInRange(t, nil, nil, ascending, false, false, func(node *Node) bool {
		return cb(node)
	})
}

// traversePost is a wrapper over traverseInRange when we want the whole tree post-order
func (node *Node) traversePost(t *ImmutableTree, ascending bool, cb func(*Node) bool) bool {
	return node.traverseInRange(t, nil, nil, ascending, false, true, func(node *Node) bool {
		return cb(node)
	})
}

func (node *Node) traverseInRange(tree *ImmutableTree, start, end []byte, ascending bool, inclusive bool, post bool, cb func(*Node) bool) bool {
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
func (node *Node) lmd(t *ImmutableTree) *Node {
	if node.isLeaf() {
		return node
	}
	return t.getLeftChild(node).lmd(t)
}
