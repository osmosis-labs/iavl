package cache

import "container/list"

type lruCacheWithBytesLimit struct {
	lruCache
	bytesLimit int
	curBytesEstimate int
}

func NewWithBytesLimit(bytesLimit int) Cache {
	return &lruCacheWithBytesLimit{
		lruCache: lruCache{
			dict:       make(map[string]*list.Element),
			ll:         list.New(),
		},
		bytesLimit: bytesLimit,
	}
}

func (c *lruCacheWithBytesLimit) isOverLimit() bool {
	return c.curBytesEstimate > c.bytesLimit
}

