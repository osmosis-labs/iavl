package cache_test

import (
	"testing"

	"github.com/cosmos/iavl/cache"
	"github.com/stretchr/testify/require"
)

func Test_BytesLimit_Add(t *testing.T) {
	testcases := map[string]func() testcase{
		"add 1 node with size of exactly limit - added": func() testcase {
			const nodeIdx = 0

			return testcase{
				cacheLimit: testNodes[nodeIdx].GetFullSize() + cache.GetCacheElemMetadataSize(),
				cacheOps: []cacheOp{
					{
						testNodexIdx:       0,
						expectedResult:     noneRemoved,
						expectedBytesLimit: testNodes[nodeIdx].GetFullSize() + cache.GetCacheElemMetadataSize(),
					},
				},
				expectedNodeIndexes: []int{nodeIdx},
			}
		},
		"add 2 nodes with latter exceeding limit - added, old removed": func() testcase {
			const nodeIdx = 0

			return testcase{
				cacheLimit: testNodes[nodeIdx].GetFullSize() + cache.GetCacheElemMetadataSize(),
				cacheOps: []cacheOp{
					{
						testNodexIdx:       nodeIdx,
						expectedResult:     noneRemoved,
						expectedBytesLimit: testNodes[nodeIdx].GetFullSize() + cache.GetCacheElemMetadataSize(),
					},
					{
						testNodexIdx:       nodeIdx + 1,
						expectedResult:     nodeIdx,
						expectedBytesLimit: testNodes[nodeIdx+1].GetFullSize() + cache.GetCacheElemMetadataSize(),
					},
				},
				expectedNodeIndexes: []int{nodeIdx + 1},
			}
		},
		"add 2 nodes under limit": func() testcase {
			const nodeIdx = 0

			return testcase{
				cacheLimit: testNodes[nodeIdx].GetFullSize() + testNodes[nodeIdx+3].GetFullSize() + 2*cache.GetCacheElemMetadataSize(),
				cacheOps: []cacheOp{
					{
						testNodexIdx:       nodeIdx,
						expectedResult:     noneRemoved,
						expectedBytesLimit: testNodes[nodeIdx].GetFullSize() + cache.GetCacheElemMetadataSize(),
					},
					{
						testNodexIdx:       nodeIdx + 3,
						expectedResult:     noneRemoved,
						expectedBytesLimit: testNodes[nodeIdx].GetFullSize() + testNodes[nodeIdx+3].GetFullSize() + 2*cache.GetCacheElemMetadataSize(),
					},
				},
				expectedNodeIndexes: []int{nodeIdx, nodeIdx + 3},
			}
		},
		"add 3 nodes and 4th requiring the removal of first three due to being too large": func() testcase {
			const nodeIdx = 0

			return testcase{
				cacheLimit: testNodes[nodeIdx].GetFullSize() +
					testNodes[nodeIdx+1].GetFullSize() +
					testNodes[nodeIdx+2].GetFullSize() +
					3*cache.GetCacheElemMetadataSize(),
				cacheOps: []cacheOp{
					{
						testNodexIdx:       nodeIdx,
						expectedResult:     noneRemoved,
						expectedBytesLimit: testNodes[nodeIdx].GetFullSize() + cache.GetCacheElemMetadataSize(),
					},
					{
						testNodexIdx:       nodeIdx + 1,
						expectedResult:     noneRemoved,
						expectedBytesLimit: testNodes[nodeIdx].GetFullSize() + testNodes[nodeIdx+1].GetFullSize() + 2*cache.GetCacheElemMetadataSize(),
					},
					{
						testNodexIdx:       nodeIdx + 2,
						expectedResult:     noneRemoved,
						expectedBytesLimit: testNodes[nodeIdx].GetFullSize() + testNodes[nodeIdx+1].GetFullSize() + testNodes[nodeIdx+2].GetFullSize() + 3*cache.GetCacheElemMetadataSize(),
					},
					{
						testNodexIdx:       nodeIdx + 3,
						expectedResult:     allButLastRemoved,
						expectedBytesLimit: testNodes[nodeIdx+2].GetFullSize() + testNodes[nodeIdx+3].GetFullSize() + 2*cache.GetCacheElemMetadataSize(),
					},
				},
				expectedNodeIndexes: []int{nodeIdx + 2, nodeIdx + 3},
			}
		},
	}

	for name, getTestcaseFn := range testcases {
		t.Run(name, func(t *testing.T) {
			tc := getTestcaseFn()
			bytesLimitCache := cache.NewWithBytesLimit(tc.cacheLimit)
			testAdd(t, bytesLimitCache, tc)
		})
	}
}

func Test_BytesLimitCache_Remove(t *testing.T) {
	testcases := map[string]func() testcase{
		"remove non-existent key, cache limit 0 - nil returned": func() testcase {
			const (
				nodeIdx    = 0
				cacheLimit = 0
			)

			return testcase{
				cacheLimit: cacheLimit,
				cacheOps: []cacheOp{
					{
						testNodexIdx:   nodeIdx,
						expectedResult: noneRemoved,
					},
				},
			}
		},
		"remove non-existent key - nil returned": func() testcase {
			const (
				nodeIdx = 0
			)

			var (
				cacheLimit = testNodes[nodeIdx].GetFullSize() + cache.GetCacheElemMetadataSize()
			)

			return testcase{
				setup: func(c cache.Cache) {
					require.Empty(t, cache.MockAdd(c, testNodes[nodeIdx+1]))
					require.Equal(t, 1, c.Len())
				},
				cacheLimit: cacheLimit,
				cacheOps: []cacheOp{
					{
						testNodexIdx:   nodeIdx,
						expectedResult: noneRemoved,
					},
				},
				expectedNodeIndexes: []int{1},
			}
		},
		"remove existent key - removed": func() testcase {
			const (
				nodeIdx = 0
			)

			var (
				cacheLimit = testNodes[nodeIdx].GetFullSize() + cache.GetCacheElemMetadataSize()
			)

			return testcase{
				setup: func(c cache.Cache) {
					require.Empty(t, cache.MockAdd(c, testNodes[nodeIdx]))
					require.Equal(t, 1, c.Len())
				},
				cacheLimit: cacheLimit,
				cacheOps: []cacheOp{
					{
						testNodexIdx:   nodeIdx,
						expectedResult: nodeIdx,
					},
				},
			}
		},
		"remove twice, cache limit 1 - removed first time, then nil": func() testcase {
			const (
				nodeIdx = 0
			)

			var (
				cacheLimit = testNodes[nodeIdx].GetFullSize() + cache.GetCacheElemMetadataSize()
			)

			return testcase{
				setup: func(c cache.Cache) {
					require.Empty(t, cache.MockAdd(c, testNodes[nodeIdx]))
					require.Equal(t, 1, c.Len())
				},
				cacheLimit: cacheLimit,
				cacheOps: []cacheOp{
					{
						testNodexIdx:   nodeIdx,
						expectedResult: nodeIdx,
					},
					{
						testNodexIdx:   nodeIdx,
						expectedResult: noneRemoved,
					},
				},
			}
		},
		"remove all, cache limit 3": func() testcase {
			const (
				nodeIdx = 0
			)

			var (
				cacheLimit = testNodes[nodeIdx].GetFullSize() +
					testNodes[nodeIdx+1].GetFullSize() +
					testNodes[nodeIdx+2].GetFullSize() +
					3*cache.GetCacheElemMetadataSize()
			)

			return testcase{
				setup: func(c cache.Cache) {
					require.Empty(t, cache.MockAdd(c, testNodes[nodeIdx]))
					require.Empty(t, cache.MockAdd(c, testNodes[nodeIdx+1]))
					require.Empty(t, cache.MockAdd(c, testNodes[nodeIdx+2]))
					require.Equal(t, 3, c.Len())
				},
				cacheLimit: cacheLimit,
				cacheOps: []cacheOp{
					{
						testNodexIdx:   nodeIdx + 2,
						expectedResult: nodeIdx + 2,
					},
					{
						testNodexIdx:   nodeIdx,
						expectedResult: nodeIdx,
					},
					{
						testNodexIdx:   nodeIdx + 1,
						expectedResult: nodeIdx + 1,
					},
				},
			}
		},
	}

	for name, getTestcaseFn := range testcases {
		t.Run(name, func(t *testing.T) {
			tc := getTestcaseFn()
			bytesLimitCache := cache.NewWithNodeLimit(tc.cacheLimit)
			testRemove(t, bytesLimitCache, tc)
		})
	}
}