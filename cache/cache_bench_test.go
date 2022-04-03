package cache_test

import (
	"crypto/rand"
	"testing"

	"github.com/cosmos/iavl/cache"
	"github.com/cosmos/iavl/common"
	"github.com/stretchr/testify/require"
)

type benchTestcase struct {
	cacheLimit int
	keySize    int
}

func Benchmark_NodeLimitCache_Add(b *testing.B) {
	b.ReportAllocs()
	testcases := map[string]benchTestcase{
		"small - limit: 10K, key size - 10b": {
			cacheLimit: 10000,
			keySize:    10,
		},
		"med - limit: 100K, key size 20b": {
			cacheLimit: 100000,
			keySize:    20,
		},
		"large - limit: 1M, key size 30b": {
			cacheLimit: 1000000,
			keySize:    30,
		},
	}

	benchmarkAdd(b, testcases)
}

func benchmarkAdd(b *testing.B, testcases map[string]benchTestcase) {
	for name, tc := range testcases {
		c := cache.NewWithNodeLimit(tc.cacheLimit)
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				cache.Add(c, &testNode{
					key: randBytes(tc.keySize),
				})
			}
		})
	}
}

func Benchmark_NodeLimitCache_Remove(b *testing.B) {
	c := cache.NewWithNodeLimit(1000)
	bencmarkRemove(b, c)
}

func Benchmark_BytesLimitCache_Add(b *testing.B) {
	b.ReportAllocs()
	testcases := map[string]benchTestcase{
		"small - limit: 50MB, key size - 10b": {
			cacheLimit: 50 * 1024 * 1024,
			keySize:    10,
		},
		"med - limit: 100MB, key size 20b": {
			cacheLimit: 100 * 1024 * 1024,
			keySize:    20,
		},
		"large - limit 500MB: , key size 30b": {
			cacheLimit: 500 * 1024 * 1024,
			keySize:    30,
		},
	}

	benchmarkAdd(b, testcases)
}

func Benchmark_BytesLimitCache_Remove(b *testing.B) {
	c := cache.NewWithNodeLimit(50 * 1024 * 1024)
	bencmarkRemove(b, c)
}

// benchmarkRemove is meant to be run manually
// This is done because we want to avoid removing non-existent keys
// As a result, -benchtime flah should be below keysToPopulate variable.
// To run, uncomment b.Skip() and execute:
// go test -run=^$ -bench ^Benchmark_BytesLimitCache_Remove$ github.com/cosmos/iavl/cache -benchtime=1000000x
func bencmarkRemove(b *testing.B, c cache.Cache) {
	b.Skip()
	b.ReportAllocs()
	b.StopTimer()
	existentKeyMirror := [][]byte{}
	// Populate cache
	const keysToPopulate = 1000000

	for i := 0; i < keysToPopulate; i++ {
		key := randBytes(20)

		existentKeyMirror = append(existentKeyMirror, key)

		cache.Add(c, &testNode{
			key: key,
		})
	}

	r := common.NewRand()

	removedKeys := make(map[string]struct{}, 0)

	for i := 0; i < b.N; i++ {
		var key []byte
		for len(removedKeys) != keysToPopulate {
			key = existentKeyMirror[r.Intn(len(existentKeyMirror))]
			if _, exists := removedKeys[string(key)]; !exists {
				break
			}
		}

		b.ResetTimer()
		removed := cache.Remove(c, key)
		removedKeys[string(key)] = struct{}{}
		require.NotNil(b, removed)
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
