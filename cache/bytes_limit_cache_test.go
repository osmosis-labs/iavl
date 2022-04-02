package cache_test

import (
	"testing"

	"github.com/cosmos/iavl/cache"
	"github.com/stretchr/testify/require"
)

func Test_BytesLimit_Add(t *testing.T) {
	testcases := map[string]func() testcase {
		"add 1 node with size of exactly limit - added": func () testcase  {
			const nodeIdx = 0

			return testcase{
				cacheLimit: testNodes[nodeIdx].GetFullSize() + cache.GetCacheElemMetadataSize(),
				cacheOps: []cacheOp{
					{
						testNodexIdx:   0,
						expectedResult: noneRemoved,
					},
				},
				expectedNodeIndexes: []int{nodeIdx},
			}
		},
		"add 2 nodes with latter exceeding limit - added, old removed": func () testcase  {
			const nodeIdx = 0

			return testcase{
				cacheLimit: testNodes[nodeIdx].GetFullSize() + cache.GetCacheElemMetadataSize(),
				cacheOps: []cacheOp{
					{
						testNodexIdx:   nodeIdx,
						expectedResult: noneRemoved,
					},
					{
						testNodexIdx:   nodeIdx + 1,
						expectedResult: nodeIdx,
					},
				},
				expectedNodeIndexes: []int{nodeIdx + 1},
			}
		},
	}

	for name, getTestcaseFn := range testcases {
		t.Run(name, func(t *testing.T) {
			tc := getTestcaseFn()

			c := cache.NewWithBytesLimit(tc.cacheLimit)

			expectedCurSize := 0

			for _, op := range tc.cacheOps {

				actualResult := cache.MockAdd(c, testNodes[op.testNodexIdx])

				expectedResult := op.expectedResult

				if expectedResult == noneRemoved {
					require.Empty(t, actualResult)
					expectedCurSize++
				} else {
					require.NotNil(t, actualResult)

					// Here, op.expectedResult represents the index of the removed node in tc.cacheOps
					require.Contains(t, actualResult, testNodes[int(op.expectedResult)])
				}
				require.Equal(t, expectedCurSize, c.Len())
			}

			validateCacheContentsAfterTest(t, tc, c)
		})
	}
}
