package cache

// Node represents a node eligible for caching.
type Node interface {
	GetKey() []byte
}

// Cache is an in-memory structure to persist nodes for quick access.
type Cache interface {
	// Adds node to cache. If full and had to remove the oldest element,
	// returns the oldest, otherwise nil.
	Add(node Node) Node

	// Returns Node for the key, if exists. nil otherwise.
	Get(key []byte) Node

	// Has returns true if node with key exists in cache, false otherwise.
	Has(key []byte) bool

	// Remove removes node with key from cache. The removed node is returned.
	// if not in cache, return nil.
	Remove(key []byte) Node

	// Len returns the cache length.
	Len() int
}
