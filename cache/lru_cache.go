package cache

import (
	"container/list"
)

// lruCache is an abstract LRU cache implementation with no limits.
type lruCache struct {
	dict map[string]*list.Element // FastNode cache.
	ll   *list.List               // LRU queue of cache elements. Used for deletion.
}

var _ Cache = (*lruCache)(nil)

func (c *lruCache) add(node Node) Node {
	if e, exists := c.dict[string(node.GetKey())]; exists {
		c.ll.MoveToFront(e)
		old := e.Value
		e.Value = node
		return old.(Node)
	}

	elem := c.ll.PushFront(node)
	c.dict[string(node.GetKey())] = elem
	return nil
}

func (c *lruCache) Get(key []byte) Node {
	if elem := c.get(key); elem != nil {
		return elem.Value.(Node)
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

func (c *lruCache) get(key []byte) *list.Element {
	elem, exists := c.dict[string(key)]
	if exists {
		c.ll.MoveToFront(elem)
		return elem
	}
	return nil
}

func (c *lruCache) remove(e *list.Element) Node {
	removed := c.ll.Remove(e).(Node)
	delete(c.dict, string(removed.GetKey()))
	return removed
}

func (c *lruCache) getOldest() *list.Element {
	return c.ll.Back()
}

func (c *lruCache) isOverLimit() bool {
	return false
}
