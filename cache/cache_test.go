package cache_test

import (
	"fmt"
	"testing"

	"github.com/cosmos/iavl/cache"
	"github.com/stretchr/testify/require"
)

// expectedResult represents the expected result of each add/remove operation.
// It can be noneRemoved or the index of the removed node in testNodes
type expectedResult int
const (
	noneRemoved expectedResult = -1
	// The rest represent the index of the removed node
)

// testNode is the node used for testing cache implementation
type testNode struct {
	key   []byte
}

func (tn *testNode) GetKey() []byte {
	return tn.key
}


const (
	testKey = "key"
)

var _ cache.Node = (*testNode)(nil)

var (
	testNodes = []cache.Node{
		&testNode{
			key: []byte(fmt.Sprintf("%s%d", testKey, 1)),
		},
		&testNode{
			key: []byte(fmt.Sprintf("%s%d", testKey, 2)),
		},
		&testNode{
			key: []byte(fmt.Sprintf("%s%d", testKey, 3)),
		},
	}
)

func Test_Cache_Add(t *testing.T) {
	
	type cacheOp struct {
		testNodexIdx             int
		expectedResult   expectedResult
	}

	testcases := map[string]struct {
		cacheLimit          int
		cacheOps            []cacheOp
		expectedNodeIndexes []int // contents of the cache once test case completes represent by indexes in testNodes
	}{
		"add 1 node with 1 limit - added": {
			cacheLimit: 1,
			cacheOps: []cacheOp{
				{
					testNodexIdx: 0,
					expectedResult: noneRemoved,
				},
			},
			expectedNodeIndexes: []int{0},
		},
		"add 1 node with 0 limit - not added and return itself": {
			cacheLimit: 0,
			cacheOps: []cacheOp{
				{
					testNodexIdx: 0,
					expectedResult: 0,
				},
			},
		},
		"add 3 nodes with 1 limit - first 2 removed": {
			cacheLimit: 1,
			cacheOps: []cacheOp{
				{
					testNodexIdx: 0,
					expectedResult: noneRemoved,
				},
				{
					testNodexIdx: 1,
					expectedResult: 0,
				},
				{
					testNodexIdx: 2,
					expectedResult: 1,
				},
			},
			expectedNodeIndexes: []int{2},
		},
		"add 3 nodes with 2 limit - first removed": {
			cacheLimit: 2,
			cacheOps: []cacheOp{
				{
					testNodexIdx: 0,
					expectedResult: noneRemoved,
				},
				{
					testNodexIdx: 1,
					expectedResult: noneRemoved,
				},
				{
					testNodexIdx: 2,
					expectedResult: 0,
				},
			},
			expectedNodeIndexes: []int{1, 2},
		},
		"add 3 nodes with 10 limit - non removed": {
			cacheLimit: 10,
			cacheOps: []cacheOp{
				{
					testNodexIdx: 0,
					expectedResult: noneRemoved,
				},
				{
					testNodexIdx: 1,
					expectedResult: noneRemoved,
				},
				{
					testNodexIdx: 2,
					expectedResult: noneRemoved,
				},
			},
			expectedNodeIndexes: []int{0, 1, 2},
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			cache := cache.New(tc.cacheLimit)

			expectedCurSize := 0

			for _, op := range tc.cacheOps {

				actualResult := cache.Add(testNodes[op.testNodexIdx])

				expectedResult := op.expectedResult

				if expectedResult == noneRemoved {
					require.Nil(t, actualResult)
					expectedCurSize++
				} else {
					require.NotNil(t, actualResult)
					
					// Here, op.expectedResult represents the index of the removed node in tc.cacheOps
					require.Equal(t, testNodes[int(op.expectedResult)], actualResult)
				}
				require.Equal(t, expectedCurSize, cache.Len())
			}

			require.Equal(t, len(tc.expectedNodeIndexes), cache.Len())
			for _, idx := range tc.expectedNodeIndexes {
				require.True(t, cache.Has(testNodes[idx].GetKey()))
			}
		})
	}
}
