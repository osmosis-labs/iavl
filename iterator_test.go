package iavl

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	dbm "github.com/tendermint/tm-db"
)

func TestIterator_NewIterator_NilTree_Failure(t *testing.T) {
	var start, end []byte = []byte{'a'}, []byte{'c'}
	ascending := true
	itr := NewIterator(start, end, ascending, nil)

	require.NotNil(t, itr)
	require.False(t, itr.Valid())
	actualsStart, actualEnd := itr.Domain()
	require.Equal(t, start, actualsStart)
	require.Equal(t, end, actualEnd)
	require.Error(t, itr.Error())
}

func TestIterator_Empty_Invalid(t *testing.T) {
	config := &iteratorTestConfig{
		startByteToSet: 'a',
		endByteToSet: 'z',
		startIterate: []byte("a"),
		endIterate: []byte("a"),
		ascending: true,
	}

	itr, mirror := setupIteratorAndMirror(t, config)

	require.Equal(t, 0, len(mirror))
	require.False(t, itr.Valid())
}

func TestIterator_Basic_Ranged_Ascending_Success(t *testing.T) {
	config := &iteratorTestConfig{
		startByteToSet: 'a',
		endByteToSet: 'z',
		startIterate: []byte("e"),
		endIterate: []byte("w"),
		ascending: true,
	}

	itr, mirror := setupIteratorAndMirror(t, config)
	require.True(t, itr.Valid())

	actualStart, actualEnd := itr.Domain()
	require.Equal(t, config.startIterate, actualStart)
	require.Equal(t, config.endIterate, actualEnd)

	require.NoError(t, itr.Error())

	assertIterator(t, itr, mirror, config.ascending)
}

func TestIterator_Basic_Ranged_Descending_Success(t *testing.T) {
	config := &iteratorTestConfig{
		startByteToSet: 'a',
		endByteToSet: 'z',
		startIterate: []byte("e"),
		endIterate: []byte("w"),
		ascending: false,
	}

	itr, mirror := setupIteratorAndMirror(t, config)
	require.True(t, itr.Valid())

	actualStart, actualEnd := itr.Domain()
	require.Equal(t, config.startIterate, actualStart)
	require.Equal(t, config.endIterate, actualEnd)

	require.NoError(t, itr.Error())

	assertIterator(t, itr, mirror, config.ascending)
}

func TestIterator_Basic_Full_Ascending_Success(t *testing.T) {
	config := &iteratorTestConfig{
		startByteToSet: 'a',
		endByteToSet: 'z',
		startIterate: nil,
		endIterate: nil,
		ascending: true,
	}

	itr, mirror := setupIteratorAndMirror(t, config)
	require.True(t, itr.Valid())
	require.Equal(t, 25, len(mirror))

	actualStart, actualEnd := itr.Domain()
	require.Equal(t, config.startIterate, actualStart)
	require.Equal(t, config.endIterate, actualEnd)

	require.NoError(t, itr.Error())

	assertIterator(t, itr, mirror, config.ascending)
}

func TestIterator_Basic_Full_Descending_Success(t *testing.T) {
	config := &iteratorTestConfig{
		startByteToSet: 'a',
		endByteToSet: 'z',
		startIterate: nil,
		endIterate: nil,
		ascending: false,
	}

	itr, mirror := setupIteratorAndMirror(t, config)
	require.True(t, itr.Valid())
	require.Equal(t, 25, len(mirror))

	actualStart, actualEnd := itr.Domain()
	require.Equal(t, config.startIterate, actualStart)
	require.Equal(t, config.endIterate, actualEnd)

	require.NoError(t, itr.Error())

	assertIterator(t, itr, mirror, config.ascending)
}

func TestIterator_WithDelete_Full_Ascending_Success(t *testing.T) {
	config := &iteratorTestConfig{
		startByteToSet: 'a',
		endByteToSet: 'z',
		startIterate: nil,
		endIterate: nil,
		ascending: false,
	}

	tree, mirror := getRandomizedTreeAndMirror(t)

	_, _, err := tree.SaveVersion()
	require.NoError(t, err)
	
	randomizeTreeAndMirror(t, tree, mirror)

	_, _, err = tree.SaveVersion()
	require.NoError(t, err)

	err = tree.DeleteVersion(1)
	require.NoError(t, err)

	immutableTree, err := tree.GetImmutable(tree.ndb.getLatestVersion())
	require.NoError(t, err)

	itr := NewIterator(config.startIterate, config.endIterate, config.ascending, immutableTree)
	require.True(t, itr.Valid())

	// sort mirror for assertion
	sortedMirror := make([][]string, 0, len(mirror))
	for k, v := range mirror {
		sortedMirror = append(sortedMirror, []string{k, v})
	}

	sort.Slice(sortedMirror, func(i, j int) bool {
		return sortedMirror[i][0] < sortedMirror[j][0]
	})

	assertIterator(t, itr, sortedMirror, config.ascending)
}

func setupIteratorAndMirror(t *testing.T, config *iteratorTestConfig) (dbm.Iterator, [][]string) {
	tree, err := NewMutableTree(dbm.NewMemDB(), 0)
	require.NoError(t, err)

	mirror := setupMirrorForIterator(t, config, tree)

	immutableTree, err := tree.GetImmutable(tree.ndb.getLatestVersion())
	require.NoError(t, err)

	itr := NewIterator(config.startIterate, config.endIterate, config.ascending, immutableTree)
	return itr, mirror
}