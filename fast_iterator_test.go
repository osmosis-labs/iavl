package iavl

import (
	"testing"

	"github.com/stretchr/testify/require"
	dbm "github.com/tendermint/tm-db"
)

func TestFastIterator_Empty_Invalid(t *testing.T) {
	// config := &iteratorTestConfig{
	// 	startByteToSet: 'a',
	// 	endByteToSet: 'z',
	// 	startIterate: []byte("a"),
	// 	endIterate: []byte("a"),
	// 	ascending: true,
	// }

	// itr, mirror := setupIteratorAndMirror(t, config)

	// require.Equal(t, 0, len(mirror))
	// require.False(t, itr.Valid())
}

func TestFastIterator_Basic_Ranged_Ascending_Success(t *testing.T) {
	config := &iteratorTestConfig{
		startByteToSet: 'a',
		endByteToSet: 'z',
		startIterate: []byte("e"),
		endIterate: []byte("w"),
		ascending: true,
	}

	itr, mirror := setupFastIteratorAndMirror(t, config)
	require.True(t, itr.Valid())

	actualStart, actualEnd := itr.Domain()
	require.Equal(t, config.startIterate, actualStart)
	require.Equal(t, config.endIterate, actualEnd)

	require.NoError(t, itr.Error())

	assertIterator(t, itr, mirror, config.ascending)
}

func TestFastIterator_Basic_Ranged_Descending_Success(t *testing.T) {
}

func TestFastIterator_Basic_AnyRange_Success(t *testing.T) {
}

func TestFastIterator_New_InvalidRange_Failure(t *testing.T) {
	tree, err := NewMutableTree(dbm.NewMemDB(), 0)
	require.NoError(t, err)

	start := []byte("b")
	end := []byte("a")

	isUpdated := tree.Set(start, []byte("value1"))
	require.False(t, isUpdated)
	isUpdated = tree.Set(end, []byte("value2"))
	require.False(t, isUpdated)
	_, _,  err = tree.SaveVersion()
	require.NoError(t, err)

	itr := NewFastIterator(start, end, true, tree.ndb)
	require.False(t, itr.Valid())
}

func TestFastterator_ImmutableTree_Partialteration_WithDelete(t *testing.T) {

}

func setupFastIteratorAndMirror(t *testing.T, config *iteratorTestConfig) (dbm.Iterator, [][]string) {
	tree, err := NewMutableTree(dbm.NewMemDB(), 0)
	require.NoError(t, err)

	mirror := setupMirrorForIterator(t,config, tree)

	itr := NewFastIterator(config.startIterate, config.endIterate, config.ascending, tree.ndb)
	return itr, mirror
}
