package cache

import (
	"container/list"
	"errors"

	"github.com/cosmos/iavl/common"
)

type lruCacheWithBytesLimit struct {
	lruCache
	bytesLimit int
	// This is called an estimate because we calculcate the number of bytes used based on the
	// knowledge of how the underlying structs, slices and strings are represented in Go.
	// Since Go is a garbage collected language, there is no good way to retrieve the exact number
	// of bytes used by both the stack and the heap at any particular moment. As a result, this
	// is named an estimate to reflect that it is not a precise number.
	curBytesEstimate int
}

var _ Cache = (*lruCacheWithBytesLimit)(nil)

func NewWithBytesLimit(bytesLimit int) Cache {
	return &lruCacheWithBytesLimit{
		lruCache: lruCache{
			dict: make(map[string]*list.Element),
			ll:   list.New(),
		},
		bytesLimit: bytesLimit,
	}
}

func (c *lruCacheWithBytesLimit) GetType() Type {
	return LRU_bytes_limit
}

func (c *lruCacheWithBytesLimit) isOverLimit() bool {
	return c.curBytesEstimate > c.bytesLimit
}

func (c *lruCacheWithBytesLimit) add(node Node) {
	c.curBytesEstimate += (node.GetFullSize() + getCacheElemMetadataSize())
	c.lruCache.add(node)
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
	return common.GetStringSizeBytes() + // cache dict key
		common.UintSizeBytes + // pointer to the element in dict
		common.Uint64Size*4 // 4 pointers within list.Element
}

// getCacheCurrentBytes returns the current bytes
// estimate of the cache if the cache is lruCacheWithBytesLimit.
// If not, returns 0 and error.
func getCacheCurrentBytes(c Cache) (int, error) {
	withBytesLimit, ok := c.(*lruCacheWithBytesLimit)
	if ok {
		return withBytesLimit.curBytesEstimate, nil
	}
	return 0, errors.New("cannot get bytes limit, not lruCacheWithBytesLimit")
}