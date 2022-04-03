package cache

import "container/list"

type lruCacheWithNodeLimit struct {
	lruCache
	nodeLimit int
}

var _ Cache = (*lruCacheWithNodeLimit)(nil)

func NewWithNodeLimit(nodeLimit int) Cache {
	return &lruCacheWithNodeLimit{
		lruCache: lruCache{
			dict: make(map[string]*list.Element),
			ll:   list.New(),
		},
		nodeLimit: nodeLimit,
	}
}

func (c *lruCacheWithNodeLimit) isOverLimit() bool {
	return c.lruCache.ll.Len() > c.nodeLimit
}
