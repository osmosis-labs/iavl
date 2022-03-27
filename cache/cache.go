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

	// Has returns true if node with key exists in cache, false otherwise.
	Has(key []byte) bool

	// Remove removes node with key from cache. The removed node is returned.
	// if not in cache, return nil.
	Remove(key []byte) Node

	// Len returns the cache length.
	Len() int
}

type abstractCache struct {
	dict       map[string]*list.Element // FastNode cache.
	cacheLimit int                      // FastNode cache size limit in elements.
	queue      *list.List               // LRU queue of cache elements. Used for deletion.
}

var _ Cache = (*abstractCache)(nil)

func New(cacheLimit int) Cache {
	return &abstractCache{
		dict:       make(map[string]*list.Element),
		cacheLimit: cacheLimit,
		queue:      list.New(),
	}
}

func (nc *abstractCache) Add(node Node) Node {
	elem := nc.queue.PushBack(node)
	nc.dict[string(node.GetKey())] = elem

	if nc.queue.Len() > nc.cacheLimit {
		oldest := nc.queue.Front()

		return nc.remove(oldest)
	}
	return nil
}

func (c *abstractCache) Has(key []byte) bool {
	_, ok := c.dict[string(key)]
	return ok
}

func (nc *abstractCache) Len() int {
	return nc.queue.Len()
}

func (c *abstractCache) Remove(key []byte) Node {
	if elem, ok := c.dict[string(key)]; ok {
		return c.remove(elem)
	}
	return nil
}

func (c *abstractCache) remove(e *list.Element) Node {
	removed := c.queue.Remove(e).(Node)
	delete(c.dict, string(removed.GetKey()))
	return removed
}
