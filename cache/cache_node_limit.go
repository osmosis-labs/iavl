package cache

import "container/list"

// lruCacheNodeLimit is an LRU cache implementation.
type lruCacheNodeLimit struct {
	dict       map[string]*list.Element // FastNode cache.
	cacheLimit int                      // FastNode cache size limit in elements.
	ll         *list.List               // LRU queue of cache elements. Used for deletion.
}

var _ Cache = (*lruCacheNodeLimit)(nil)

func NewWithNodeLimit(cacheLimit int) Cache {
	return &lruCacheNodeLimit{
		dict:       make(map[string]*list.Element),
		cacheLimit: cacheLimit,
		ll:         list.New(),
	}
}

func (c *lruCacheNodeLimit) Add(node Node) Node {
	if e, exists := c.dict[string(node.GetKey())]; exists {
		c.ll.MoveToFront(e)
		old := e.Value
		e.Value = node
		return old.(Node)
	}

	elem := c.ll.PushFront(node)
	c.dict[string(node.GetKey())] = elem

	if c.ll.Len() > c.cacheLimit {
		oldest := c.ll.Back()

		return c.remove(oldest)
	}
	return nil
}

func (nc *lruCacheNodeLimit) Get(key []byte) Node {
	if ele, hit := nc.dict[string(key)]; hit {
		nc.ll.MoveToFront(ele)
		return ele.Value.(Node)
	}
	return nil
}

func (c *lruCacheNodeLimit) Has(key []byte) bool {
	_, exists := c.dict[string(key)]
	return exists
}

func (nc *lruCacheNodeLimit) Len() int {
	return nc.ll.Len()
}

func (c *lruCacheNodeLimit) Remove(key []byte) Node {
	if elem, exists := c.dict[string(key)]; exists {
		return c.remove(elem)
	}
	return nil
}

func (c *lruCacheNodeLimit) remove(e *list.Element) Node {
	removed := c.ll.Remove(e).(Node)
	delete(c.dict, string(removed.GetKey()))
	return removed
}
