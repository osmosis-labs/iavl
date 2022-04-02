package cache

// Node represents a node eligible for caching.
type Node interface {
	// TODO: bytes
	GetKey() []byte
	// TODO: bytes
	GetFullSize() int
}

// Cache is an in-memory structure to persist nodes for quick access.
type Cache interface {


	// Returns Node for the key, if exists. nil otherwise.
	Get(key []byte) Node

	// Has returns true if node with key exists in cache, false otherwise.
	Has(key []byte) bool

	// Remove removes node with key from cache. The removed node is returned.
	// if not in cache, return nil.
	Remove(key []byte) Node

	// Len returns the cache length.
	Len() int

	// add dds node to cache. If full and had to remove the oldest element,
	// returns the oldest, otherwise nil.
	add(node Node) Node

	isOverLimit() bool

	removeOldest() Node
}

func Add(c Cache, node Node) {
	if old := c.add(node); old != nil {
		return
	}

	for c.isOverLimit() {
		c.removeOldest()
	}
}

// Used for testing, returns removed Nodes
func add(c Cache, node Node) []Node  {
	if old := c.add(node); old != nil {
		return []Node{old}
	}

	removed := make([]Node, 0)
	for c.isOverLimit() {
		removed = append(removed, c.removeOldest())
	}
	return removed
}
