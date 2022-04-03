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

			c := cache.NewWithBytesLimit(tc.cacheLimit)

			expectedCurSize := 0

			for opIdx, op := range tc.cacheOps {

				actualResult := cache.MockAdd(c, testNodes[op.testNodexIdx])

				expectedResult := op.expectedResult

				switch expectedResult {
				case noneRemoved:
					require.Empty(t, actualResult)
					expectedCurSize++
				case allButLastRemoved:
					require.NotNil(t, actualResult)
					expectedCurSize = 2
					require.True(t, c.Has(testNodes[op.testNodexIdx].GetKey()))
					require.Contains(t, actualResult, testNodes[tc.cacheOps[opIdx-2].testNodexIdx])
				default:
					require.NotNil(t, actualResult)
					// Here, op.expectedResult represents the index of the removed node in tc.cacheOps
					require.Contains(t, actualResult, testNodes[int(op.expectedResult)])
				}
				require.Equal(t, expectedCurSize, c.Len())

				currentBytes, err := cache.GetCacheCurrentBytes(c)
				require.NoError(t, err)
				require.Equal(t, op.expectedBytesLimit, currentBytes)
			}

			validateCacheContentsAfterTest(t, tc, c)
		})
	}
}
