package iavl

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIterate_MutableTree_Unsaved(t *testing.T) {
	tree, mirror := getRandomizedTreeAndMirror(t)
	assertMutableMirror(t, tree, mirror)
}

func TestIterate_MutableTree_Saved(t *testing.T) {
	tree, mirror  := getRandomizedTreeAndMirror(t)

	_, _, err := tree.SaveVersion()
	require.NoError(t, err)

	assertMutableMirror(t, tree, mirror)
}

func TestIterate_MutableTree_Unsaved_NextVersion(t *testing.T) {
	tree, mirror := getRandomizedTreeAndMirror(t)

	_, _, err := tree.SaveVersion()
	require.NoError(t, err)

	assertMutableMirror(t, tree, mirror)

	tree, mirror = randomizeTreeAndMirror(t, tree, mirror)

	assertMutableMirror(t, tree, mirror)
}

func TestIterate_ImmutableTree_Version1(t *testing.T) {
	tree, mirror := getRandomizedTreeAndMirror(t)

	_, _, err := tree.SaveVersion()
	require.NoError(t, err)

	immutableTree, err := tree.GetImmutable(1)
	require.NoError(t, err)

	assertImmutableMirror(t, immutableTree, mirror)
}

func TestIterate_ImmutableTree_Version2(t *testing.T) {
	tree, mirror := getRandomizedTreeAndMirror(t)

	_, _, err := tree.SaveVersion()
	require.NoError(t, err)

	tree, mirror = randomizeTreeAndMirror(t, tree, mirror)

	_, _, err = tree.SaveVersion()
	require.NoError(t, err)

	immutableTree, err := tree.GetImmutable(2)
	require.NoError(t, err)

	assertImmutableMirror(t, immutableTree, mirror)
}

func assertMutableMirror(t *testing.T, tree *MutableTree, mirror map[string]string) {
	sortedMirrorKeys := make([]string, 0, len(mirror))
	for k := range mirror {
		sortedMirrorKeys = append(sortedMirrorKeys, k)
	}
	sort.Strings(sortedMirrorKeys)
	
	curKeyIdx := 0
	tree.Iterate(func(k, v []byte) bool {
		nextMirrorKey := sortedMirrorKeys[curKeyIdx]
		nextMirrorValue := mirror[nextMirrorKey]

		if nextMirrorKey != string(k) {
			t.Fatal("")
		}

		require.Equal(t, []byte(nextMirrorKey), k)
		require.Equal(t, []byte(nextMirrorValue), v)

		curKeyIdx++
		return false
	})
}

func assertImmutableMirror(t *testing.T, tree *ImmutableTree, mirror map[string]string) {
	sortedMirrorKeys := getSortedMirrorKeys(mirror)
	
	curKeyIdx := 0
	tree.Iterate(func(k, v []byte) bool {
		nextMirrorKey := sortedMirrorKeys[curKeyIdx]
		nextMirrorValue := mirror[nextMirrorKey]

		if nextMirrorKey != string(k) {
			t.Fatal("")
		}

		require.Equal(t, []byte(nextMirrorKey), k)
		require.Equal(t, []byte(nextMirrorValue), v)

		curKeyIdx++
		return false
	})
}

func getSortedMirrorKeys(mirror map[string]string) []string {
	sortedMirrorKeys := make([]string, 0, len(mirror))
	for k := range mirror {
		sortedMirrorKeys = append(sortedMirrorKeys, k)
	}
	sort.Strings(sortedMirrorKeys)
	return sortedMirrorKeys
}

func getRandomizedTreeAndMirror(t *testing.T) (*MutableTree, map[string]string) {
	const cacheSize = 100

	tree, err := getTestTree(cacheSize)
	require.NoError(t, err)

	mirror := make(map[string]string)

	return randomizeTreeAndMirror(t, tree, mirror)
}

func randomizeTreeAndMirror(t *testing.T, tree *MutableTree, mirror map[string]string) (*MutableTree, map[string]string) {
	const keyValLength = 5

	numberOfSets := 1000
	numberOfUpdates := numberOfSets / 4
	numberOfRemovals := numberOfSets / 4

	for numberOfSets > numberOfRemovals*3 {
		key := randBytes(keyValLength)
		value := randBytes(keyValLength)

		isUpdated := tree.Set(key, value)
		require.False(t, isUpdated)
		mirror[string(key)] = string(value)

		numberOfSets--
	}

	for numberOfSets+numberOfRemovals+numberOfUpdates > 0 {
		randOp := rand.Intn(2)
		if randOp == 0 && numberOfSets > 0 {

			numberOfSets--

			key := randBytes(keyValLength)
			value := randBytes(keyValLength)

			isUpdated := tree.Set(key, value)
			require.False(t, isUpdated)
			mirror[string(key)] = string(value)
		} else if randOp == 1 && numberOfUpdates > 0 {

			numberOfUpdates--

			key := getRandomKeyFrom(mirror)
			value := randBytes(keyValLength)

			isUpdated := tree.Set([]byte(key), value)
			require.True(t, isUpdated)
			mirror[string(key)] = string(value)
		} else if numberOfRemovals > 0 {

			numberOfRemovals--

			key := getRandomKeyFrom(mirror)

			val, isRemoved := tree.Remove([]byte(key))
			require.True(t, isRemoved)
			require.NotNil(t, val)
			delete(mirror, string(key))
		}
	}
	return tree, mirror
}

func getRandomKeyFrom(mirror map[string]string) string {
	keys := make([]string, 0, len(mirror))
	for k := range mirror {
		keys = append(keys, k)
	}
	key := keys[rand.Intn(len(keys))]
	return key
}
