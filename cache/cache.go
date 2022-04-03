package cache

import "container/list"

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

	// Len returns the cache length.
	Len() int

	// add adds node to cache.
	add(node Node)

	get(key []byte) *list.Element

	// remove removes node with key from cache. The removed node is returned.
	// if not in cache, return nil.
	remove(e *list.Element) Node

	isOverLimit() bool

	getOldest() *list.Element
}

func Add(c Cache, node Node) {
	c.add(node)

	for c.isOverLimit() {
		c.remove(c.getOldest())
	}
}

func Remove(c Cache, key []byte) Node {
	if elem := c.get(key); elem != nil {
		return c.remove(elem)
	}
	return nil
}
