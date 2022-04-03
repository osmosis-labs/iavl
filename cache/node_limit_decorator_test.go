package cache_test

import (
	"testing"

	"github.com/cosmos/iavl/cache"
	"github.com/stretchr/testify/require"
)

func Test_NodeLimitCache_Add(t *testing.T) {
	testcases := map[string]testcase{
		"add 1 node with 1 limit - added": {
			cacheLimit: 1,
			cacheOps: []cacheOp{
				{
					testNodexIdx:   0,
					expectedResult: noneRemoved,
				},
			},
			expectedNodeIndexes: []int{0},
		},
		"add 1 node twice, cache limit 2 - only one added": {
			cacheLimit: 2,
			cacheOps: []cacheOp{
				{
					testNodexIdx:   0,
					expectedResult: noneRemoved,
				},
				{
					testNodexIdx:   0,
					expectedResult: updated,
				},
			},
			expectedNodeIndexes: []int{0},
		},
		"add 1 node with 0 limit - not added and return itself": {
			cacheLimit: 0,
			cacheOps: []cacheOp{
				{
					testNodexIdx:   0,
					expectedResult: 0,
				},
			},
		},
		"add 3 nodes with 1 limit - first 2 removed": {
			cacheLimit: 1,
			cacheOps: []cacheOp{
				{
					testNodexIdx:   0,
					expectedResult: noneRemoved,
				},
				{
					testNodexIdx:   1,
					expectedResult: 0,
				},
				{
					testNodexIdx:   2,
					expectedResult: 1,
				},
			},
			expectedNodeIndexes: []int{2},
		},
		"add 3 nodes with 2 limit - first removed": {
			cacheLimit: 2,
			cacheOps: []cacheOp{
				{
					testNodexIdx:   0,
					expectedResult: noneRemoved,
				},
				{
					testNodexIdx:   1,
					expectedResult: noneRemoved,
				},
				{
					testNodexIdx:   2,
					expectedResult: 0,
				},
			},
			expectedNodeIndexes: []int{1, 2},
		},
		"add 3 nodes with 10 limit - non removed": {
			cacheLimit: 10,
			cacheOps: []cacheOp{
				{
					testNodexIdx:   0,
					expectedResult: noneRemoved,
				},
				{
					testNodexIdx:   1,
					expectedResult: noneRemoved,
				},
				{
					testNodexIdx:   2,
					expectedResult: noneRemoved,
				},
			},
			expectedNodeIndexes: []int{0, 1, 2},
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			nodeLimitCache := cache.NewWithNodeLimit(tc.cacheLimit)
			testAdd(t, nodeLimitCache, tc)
		})
	}
}

func Test_NodeLimitCache_Remove(t *testing.T) {
	testcases := map[string]func() testcase{
		"remove non-existent key, cache limit 0 - nil returned": func() testcase {

			return testcase{
				cacheLimit: 0,
				cacheOps: []cacheOp{
					{
						testNodexIdx:   0,
						expectedResult: noneRemoved,
					},
				},
			}
		},
		"remove non-existent key, cache limit 1 - nil returned": func() testcase {
			return testcase{
				setup: func(c cache.Cache) {
					require.Empty(t, cache.MockAdd(c, testNodes[1]))
					require.Equal(t, 1, c.Len())
				},
				cacheLimit: 1,
				cacheOps: []cacheOp{
					{
						testNodexIdx:   0,
						expectedResult: noneRemoved,
					},
				},
				expectedNodeIndexes: []int{1},
			}
		},
		"remove existent key, cache limit 1 - removed": func() testcase {

			return testcase{
				setup: func(c cache.Cache) {
					require.Empty(t, cache.MockAdd(c, testNodes[0]))
					require.Equal(t, 1, c.Len())
				},
				cacheLimit: 1,
				cacheOps: []cacheOp{
					{
						testNodexIdx:   0,
						expectedResult: 0,
					},
				},
			}
		},
		"remove twice, cache limit 1 - removed first time, then nil": func() testcase {
			return testcase{
				setup: func(c cache.Cache) {
					require.Empty(t, cache.MockAdd(c, testNodes[0]))
					require.Equal(t, 1, c.Len())
				},
				cacheLimit: 1,
				cacheOps: []cacheOp{
					{
						testNodexIdx:   0,
						expectedResult: 0,
					},
					{
						testNodexIdx:   0,
						expectedResult: noneRemoved,
					},
				},
			}
		},
		"remove all, cache limit 3": func() testcase {
			return testcase{
				setup: func(c cache.Cache) {
					require.Empty(t, cache.MockAdd(c, testNodes[0]))
					require.Empty(t, cache.MockAdd(c, testNodes[1]))
					require.Empty(t, cache.MockAdd(c, testNodes[2]))
					require.Equal(t, 3, c.Len())
				},
				cacheLimit: 3,
				cacheOps: []cacheOp{
					{
						testNodexIdx:   2,
						expectedResult: 2,
					},
					{
						testNodexIdx:   0,
						expectedResult: 0,
					},
					{
						testNodexIdx:   1,
						expectedResult: 1,
					},
				},
			}
		},
	}

	for name, getTestcaseFn := range testcases {
		t.Run(name, func(t *testing.T) {
			tc := getTestcaseFn()
			nodeLimitCache := cache.NewWithNodeLimit(tc.cacheLimit)
			testRemove(t, nodeLimitCache, tc)
		})
	}
}
