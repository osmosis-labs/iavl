package cache_test

import (
	"crypto/rand"
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
	noneRemoved expectedResult = -1
	// The rest represent the index of the removed node
)

// testNode is the node used for testing cache implementation
type testNode struct {
	key   []byte
}

type cacheOp struct {
	testNodexIdx             int
	expectedResult   expectedResult
}

type testcase struct {
	setup func(cache.Cache)
	cacheLimit          int
	cacheOps            []cacheOp
	expectedNodeIndexes []int // contents of the cache once test case completes represent by indexes in testNodes
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
	testcases := map[string]testcase{
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
		"add 1 node twice, cache limit 2 - only one added": {
			cacheLimit: 2,
			cacheOps: []cacheOp{
				{
					testNodexIdx: 0,
					expectedResult: noneRemoved,
				},
				{
					testNodexIdx: 0,
					expectedResult: 0,
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

			validateCacheContentsAfterTest(t, tc, cache)
		})
	}
}

func Test_Cache_Remove(t *testing.T) {
	testcases := map[string]testcase{
		"remove non-existent key, cache limit 0 - nil returned": {
			cacheLimit: 0,
			cacheOps: []cacheOp{
				{
					testNodexIdx: 0,
					expectedResult: noneRemoved,
				},
			},
		},
		"remove non-existent key, cache limit 1 - nil returned": {
			setup: func(c cache.Cache) {
				require.Nil(t, c.Add(testNodes[1]))
				require.Equal(t, 1, c.Len())
			},
			cacheLimit: 1,
			cacheOps: []cacheOp{
				{
					testNodexIdx: 0,
					expectedResult: noneRemoved,
				},
			},
			expectedNodeIndexes: []int{1},
		},
		"remove existent key, cache limit 1 - removed": {
			setup: func(c cache.Cache) {
				require.Nil(t, c.Add(testNodes[0]))
				require.Equal(t, 1, c.Len())
			},
			cacheLimit: 1,
			cacheOps: []cacheOp{
				{
					testNodexIdx: 0,
					expectedResult: 0,
				},
			},
		},
		"remove twice, cache limit 1 - removed first time, then nil": {
			setup: func(c cache.Cache) {
				require.Nil(t, c.Add(testNodes[0]))
				require.Equal(t, 1, c.Len())
			},
			cacheLimit: 1,
			cacheOps: []cacheOp{
				{
					testNodexIdx: 0,
					expectedResult: 0,
				},
				{
					testNodexIdx: 0,
					expectedResult: noneRemoved,
				},
			},
		},
		"remove all, cache limit 3": {
			setup: func(c cache.Cache) {
				require.Nil(t, c.Add(testNodes[0]))
				require.Nil(t, c.Add(testNodes[1]))
				require.Nil(t, c.Add(testNodes[2]))
				require.Equal(t, 3, c.Len())
			},
			cacheLimit: 3,
			cacheOps: []cacheOp{
				{
					testNodexIdx: 2,
					expectedResult: 2,
				},
				{
					testNodexIdx: 0,
					expectedResult: 0,
				},
				{
					testNodexIdx: 1,
					expectedResult: 1,
				},
			},
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			cache := cache.New(tc.cacheLimit)

			if tc.setup != nil {
				tc.setup(cache)
			}

			expectedCurSize := cache.Len()

			for _, op := range tc.cacheOps {

				actualResult := cache.Remove(testNodes[op.testNodexIdx].GetKey())

				expectedResult := op.expectedResult

				if expectedResult == noneRemoved {
					require.Nil(t, actualResult)
				} else {
					expectedCurSize--
					require.NotNil(t, actualResult)
					
					// Here, op.expectedResult represents the index of the removed node in tc.cacheOps
					require.Equal(t, testNodes[int(op.expectedResult)], actualResult)
				}
				require.Equal(t, expectedCurSize, cache.Len())
			}

			validateCacheContentsAfterTest(t, tc, cache)
		})
	}
}

func BenchmarkAdd(b *testing.B) {
	b.ReportAllocs()
	testcases := map[string]struct {
		cacheLimit          int
		keySize int
	}{
		"small - limit: 10K, key size - 10b": {
			cacheLimit: 10000,
			keySize: 10,
		},
		"med - limit: 100K, key size 20b": {
			cacheLimit: 100000,
			keySize: 20,
		},
		"large - limit: 1M, key size 30b": {
			cacheLimit: 1000000,
			keySize: 30,
		},
	}

	for name, tc := range testcases {
		cache := cache.New(tc.cacheLimit)
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = cache.Add(&testNode{
					key: randBytes(tc.keySize),
				})
			}
		})
	}
}

func BenchmarkRemove(b *testing.B) {
	b.ReportAllocs()

	b.StopTimer()
	cache := cache.New(1000)
	existentKeyMirror := [][]byte{}
	// Populate cache
	for i := 0; i < 50; i++ {
		key := randBytes(1000)

		existentKeyMirror = append(existentKeyMirror, key)

		cache.Add(&testNode{
			key: key,
		})
	}

	r := common.NewRand()

	for i := 0; i < b.N; i++ {
		key := existentKeyMirror[r.Intn(len(existentKeyMirror))]
		b.ResetTimer()
		_ = cache.Remove(key)
	}
}

func validateCacheContentsAfterTest(t *testing.T, tc testcase, cache cache.Cache) {
	require.Equal(t, len(tc.expectedNodeIndexes), cache.Len())
	for _, idx := range tc.expectedNodeIndexes {
		expectedNode := testNodes[idx]
		require.True(t, cache.Has(expectedNode.GetKey()))
		require.Equal(t, expectedNode, cache.Get(expectedNode.GetKey()))
	}
}

func randBytes(length int) []byte {
	key := make([]byte, length)
	// math.rand.Read always returns err=nil
	// we do not need cryptographic randomness for this test:
	//nolint:gosec
	rand.Read(key)
	return key
}