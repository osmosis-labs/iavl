package cache

import "container/list"

// Node represents a node eligible for caching.
type Node interface {
	// GetKey returns node's key
	GetKey() []byte

	// GetFullSize returns the number of bytes node occupies in memory.
	GetFullSize() int
}

// Cache is an in-memory structure to persist nodes for quick access.
type Cache interface {
	// Get returns Node for the key, if exists. nil otherwise.
	Get(key []byte) Node

	// Has returns true if node with key exists in cache, false otherwise.
	Has(key []byte) bool

	// Len returns the cache length.
	Len() int

	// add adds node to cache.
	add(node Node)

	// get returns list element corresponding to the key
	get(key []byte) *list.Element

	// remove removes node with key from cache. The removed node is returned.
	// if not in cache, return nil.
	remove(e *list.Element) Node

	// isOverLimit returns true if cache limit has been reached, false otherwise.
	isOverLimit() bool

	// getOldsest returns the oldest cache element.
	getOldest() *list.Element
}

// Add adds node to cache. If adding node, exceeds cache limit,
// removes old elements from cache up until the limit is not exceeded anymore.
func Add(cache Cache, node Node) {
	cache.add(node)

	for cache.isOverLimit() {
		cache.remove(cache.getOldest())
	}
}

// Remove removed node with key from cache if exists and returns the
// removed node, nil otherwise.
func Remove(cache Cache, key []byte) Node {
	if elem := cache.get(key); elem != nil {
		return cache.remove(elem)
	}
	return nil
}
