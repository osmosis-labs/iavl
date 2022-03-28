package cache

import (
	"container/list"
)

type Node interface {
	GetKey() []byte
}

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

// lruCache is an LRU cache implementeation.
type lruCache struct {
	dict       map[string]*list.Element // FastNode cache.
	cacheLimit int                      // FastNode cache size limit in elements.
	ll      *list.List               // LRU queue of cache elements. Used for deletion.
}

var _ Cache = (*lruCache)(nil)

func New(cacheLimit int) Cache {
	return &lruCache{
		dict:       make(map[string]*list.Element),
		cacheLimit: cacheLimit,
		ll:      list.New(),
	}
}

func (nc *lruCache) Add(node Node) Node {
	if e, exists := nc.dict[string(node.GetKey())]; exists {
		nc.ll.MoveToFront(e)
		old := e.Value
		e.Value = node
		return old.(Node)
	}

	elem := nc.ll.PushFront(node)
	nc.dict[string(node.GetKey())] = elem

	if nc.ll.Len() > nc.cacheLimit {
		oldest := nc.ll.Back()

		return nc.remove(oldest)
	}
	return nil
}

func (nc *lruCache) Get(key []byte) Node {
	if ele, hit := nc.dict[string(key)]; hit {
		nc.ll.MoveToFront(ele)
		return ele.Value.(Node)
	}
	return nil
}

func (c *lruCache) Has(key []byte) bool {
	_, exists := c.dict[string(key)]
	return exists
}

func (nc *lruCache) Len() int {
	return nc.ll.Len()
}

func (c *lruCache) Remove(key []byte) Node {
	if elem, exists := c.dict[string(key)]; exists {
		return c.remove(elem)
	}
	return nil
}

func (c *lruCache) remove(e *list.Element) Node {
	removed := c.ll.Remove(e).(Node)
	delete(c.dict, string(removed.GetKey()))
	return removed
}