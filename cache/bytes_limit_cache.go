package cache

import (
	"container/list"

	"github.com/cosmos/iavl/common"
)

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

func (c *lruCacheWithBytesLimit) add(node Node) Node {
	c.curBytesEstimate += (node.GetFullSize() + getCacheElemMetadataSize())
	return c.lruCache.add(node)
}

func (c *lruCacheWithBytesLimit) remove(e *list.Element) Node {
	removed := c.lruCache.remove(e)
	c.curBytesEstimate -= (removed.GetFullSize() + getCacheElemMetadataSize())
	return removed
}

// getCacheElemMetadataSize returns how much space the structures
// that hold a cache element utilize in memory.
// With the current design, a list.Element is created that consists of 4 pointers.
// In addition, a pointer to the element is stored in the Go map and has string as a key
func getCacheElemMetadataSize() int {
	return 	common.GetStringSizeBytes() + // cache dict key
	common.UintSizeBytes + // pointer to the element in dict
	common.Uint64Size * 4 // 4 pointers within list.Element
}
