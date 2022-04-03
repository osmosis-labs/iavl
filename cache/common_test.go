package cache_test

import (
	"fmt"
	"testing"

	"github.com/cosmos/iavl/cache"
	"github.com/cosmos/iavl/common"
	"github.com/stretchr/testify/require"
)

// expectedResult represents the expected result of each add/remove operation.
// It can be noneRemoved or the index of the removed node in testNodes
type expectedResult int

const (
	updated           expectedResult = -3
	allButLastRemoved expectedResult = -2
	noneRemoved       expectedResult = -1
	// The rest represent the index of the removed node
)

// testNode is the node used for testing cache implementation
type testNode struct {
	key []byte
}

type cacheOp struct {
	testNodexIdx       int
	expectedResult     expectedResult
	expectedBytesLimit int // used for testing lruCacheWithBytesLimit
}

type testcase struct {
	setup               func(cache.Cache)
	cacheLimit          int
	cacheOps            []cacheOp
	expectedNodeIndexes []int // contents of the cache once test case completes represent by indexes in testNodes
}

func (tn *testNode) GetKey() []byte {
	return tn.key
}

func (tn *testNode) GetFullSize() int {
	return len(tn.key) + common.GetSliceSizeBytes()
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
		&testNode{
			key: []byte(fmt.Sprintf("%s%d%s%d%s%d", testKey, 4, testKey, 4, testKey, 4)),
		},
	}
)

func testRemove(t *testing.T, testCache cache.Cache, tc testcase) {
	if tc.setup != nil {
		tc.setup(testCache)
	}

	expectedCurSize := testCache.Len()

	for _, op := range tc.cacheOps {

		actualResult := cache.Remove(testCache, testNodes[op.testNodexIdx].GetKey())

		expectedResult := op.expectedResult

		if expectedResult == noneRemoved {
			require.Nil(t, actualResult)
		} else {
			expectedCurSize--
			require.NotNil(t, actualResult)

			// Here, op.expectedResult represents the index of the removed node in tc.cacheOps
			require.Equal(t, testNodes[int(op.expectedResult)], actualResult)
		}
		require.Equal(t, expectedCurSize, testCache.Len())
	}

	validateCacheContentsAfterTest(t, tc, testCache)
}

func validateCacheContentsAfterTest(t *testing.T, tc testcase, cache cache.Cache) {
	require.Equal(t, len(tc.expectedNodeIndexes), cache.Len())
	for _, idx := range tc.expectedNodeIndexes {
		expectedNode := testNodes[idx]
		require.True(t, cache.Has(expectedNode.GetKey()))
		require.Equal(t, expectedNode, cache.Get(expectedNode.GetKey()))
	}
}
