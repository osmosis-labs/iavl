package iavl

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	db "github.com/tendermint/tm-db"

	"github.com/cosmos/iavl/mock"
)

func BenchmarkNodeKey(b *testing.B) {
	ndb := &nodeDB{}
	hashes := makeHashes(b, 2432325)
	for i := 0; i < b.N; i++ {
		ndb.nodeKey(hashes[i])
	}
}

func BenchmarkOrphanKey(b *testing.B) {
	ndb := &nodeDB{}
	hashes := makeHashes(b, 2432325)
	for i := 0; i < b.N; i++ {
		ndb.orphanKey(1234, 1239, hashes[i])
	}
}

func TestNewNoDbStorage_StorageVersionInDb_Success(t *testing.T) {
	const expectedVersion = defaultStorageVersionValue
	ctrl := gomock.NewController(t)
	ndb, _, _, _ := createNodeDbWithMocks(ctrl)

	require.Equal(t, expectedVersion, ndb.storageVersion)
}

func TestNewNoDbStorage_ErrorInConstructor_DefaultSet(t *testing.T) {
	const expectedVersion = defaultStorageVersionValue

	ctrl := gomock.NewController(t)
	dbMock := mock.NewMockDB(ctrl)

	dbMock.EXPECT().Get(gomock.Any()).Return(nil, errors.New("some db error")).Times(1)
	dbMock.EXPECT().NewBatch().Return(nil).Times(1)

	ndb := newNodeDB(dbMock, 0, nil)
	require.Equal(t, expectedVersion, ndb.getStorageVersion())
}

func TestNewNoDbStorage_DoesNotExist_DefaultSet(t *testing.T) {
	const expectedVersion = defaultStorageVersionValue

	ctrl := gomock.NewController(t)
	dbMock := mock.NewMockDB(ctrl)

	dbMock.EXPECT().Get(gomock.Any()).Return(nil, nil).Times(1)
	dbMock.EXPECT().NewBatch().Return(nil).Times(1)

	ndb := newNodeDB(dbMock, 0, nil)
	require.Equal(t, expectedVersion, ndb.getStorageVersion())
}

func TestSetStorageVersion_Success(t *testing.T) {
	const expectedVersion = fastStorageVersionValue

	db := db.NewMemDB()

	ndb := newNodeDB(db, 0, nil)
	require.Equal(t, defaultStorageVersionValue, ndb.getStorageVersion())

	err := ndb.setFastStorageVersionToBatch()
	require.NoError(t, err)
	require.Equal(t, expectedVersion+fastStorageVersionDelimiter+strconv.Itoa(int(ndb.getLatestVersion())), ndb.getStorageVersion())
}

func TestSetStorageVersion_DBFailure_OldKept(t *testing.T) {
	ctrl := gomock.NewController(t)
	dbMock := mock.NewMockDB(ctrl)
	batchMock := mock.NewMockBatch(ctrl)
	rIterMock := mock.NewMockIterator(ctrl)

	expectedErrorMsg := "some db error"

	expectedFastCacheVersion := 2

	dbMock.EXPECT().Get(gomock.Any()).Return([]byte(defaultStorageVersionValue), nil).Times(1)
	dbMock.EXPECT().NewBatch().Return(batchMock).Times(1)

	// rIterMock is used to get the latest version from disk. We are mocking that rIterMock returns latestTreeVersion from disk
	rIterMock.EXPECT().Valid().Return(true).Times(1)
	rIterMock.EXPECT().Key().Return(rootKeyFormat.Key(expectedFastCacheVersion)).Times(1)
	rIterMock.EXPECT().Close().Return(nil).Times(1)

	dbMock.EXPECT().ReverseIterator(gomock.Any(), gomock.Any()).Return(rIterMock, nil).Times(1)
	batchMock.EXPECT().Set(metadataKeyFormat.Key([]byte(storageVersionKey)), []byte(fastStorageVersionValue+fastStorageVersionDelimiter+strconv.Itoa(expectedFastCacheVersion))).Return(errors.New(expectedErrorMsg)).Times(1)

	ndb := newNodeDB(dbMock, 0, nil)
	require.Equal(t, defaultStorageVersionValue, ndb.getStorageVersion())

	err := ndb.setFastStorageVersionToBatch()
	require.Error(t, err)
	require.Equal(t, expectedErrorMsg, err.Error())
	require.Equal(t, defaultStorageVersionValue, ndb.getStorageVersion())
}

func TestSetStorageVersion_InvalidVersionFailure_OldKept(t *testing.T) {
	ctrl := gomock.NewController(t)
	dbMock := mock.NewMockDB(ctrl)
	batchMock := mock.NewMockBatch(ctrl)

	expectedErrorMsg := errInvalidFastStorageVersion

	invalidStorageVersion := fastStorageVersionValue + fastStorageVersionDelimiter + "1" + fastStorageVersionDelimiter + "2"

	dbMock.EXPECT().Get(gomock.Any()).Return([]byte(invalidStorageVersion), nil).Times(1)
	dbMock.EXPECT().NewBatch().Return(batchMock).Times(1)

	ndb := newNodeDB(dbMock, 0, nil)
	require.Equal(t, invalidStorageVersion, ndb.getStorageVersion())

	err := ndb.setFastStorageVersionToBatch()
	require.Error(t, err)
	require.Equal(t, expectedErrorMsg, err.Error())
	require.Equal(t, invalidStorageVersion, ndb.getStorageVersion())
}

func TestSetStorageVersion_FastVersionFirst_VersionAppended(t *testing.T) {
	db := db.NewMemDB()
	ndb := newNodeDB(db, 0, nil)
	ndb.storageVersion = fastStorageVersionValue
	ndb.latestVersion = 100

	err := ndb.setFastStorageVersionToBatch()
	require.NoError(t, err)
	require.Equal(t, fastStorageVersionValue+fastStorageVersionDelimiter+strconv.Itoa(int(ndb.latestVersion)), ndb.storageVersion)
}

func TestSetStorageVersion_FastVersionSecond_VersionAppended(t *testing.T) {
	db := db.NewMemDB()
	ndb := newNodeDB(db, 0, nil)
	ndb.latestVersion = 100

	storageVersionBytes := []byte(fastStorageVersionValue)
	storageVersionBytes[len(fastStorageVersionValue)-1]++ // increment last byte
	ndb.storageVersion = string(storageVersionBytes)

	err := ndb.setFastStorageVersionToBatch()
	require.NoError(t, err)
	require.Equal(t, string(storageVersionBytes)+fastStorageVersionDelimiter+strconv.Itoa(int(ndb.latestVersion)), ndb.storageVersion)
}

func TestSetStorageVersion_SameVersionTwice(t *testing.T) {
	db := db.NewMemDB()
	ndb := newNodeDB(db, 0, nil)
	ndb.latestVersion = 100

	storageVersionBytes := []byte(fastStorageVersionValue)
	storageVersionBytes[len(fastStorageVersionValue)-1]++ // increment last byte
	ndb.storageVersion = string(storageVersionBytes)

	err := ndb.setFastStorageVersionToBatch()
	require.NoError(t, err)
	newStorageVersion := string(storageVersionBytes) + fastStorageVersionDelimiter + strconv.Itoa(int(ndb.latestVersion))
	require.Equal(t, newStorageVersion, ndb.storageVersion)

	err = ndb.setFastStorageVersionToBatch()
	require.NoError(t, err)
	require.Equal(t, newStorageVersion, ndb.storageVersion)
}

// Test case where version is incorrect and has some extra garbage at the end
func TestShouldForceFastStorageUpdate_DefaultVersion_True(t *testing.T) {
	db := db.NewMemDB()
	ndb := newNodeDB(db, 0, nil)
	ndb.storageVersion = defaultStorageVersionValue
	ndb.latestVersion = 100

	require.False(t, ndb.shouldForceFastStorageUpgrade())
}

func TestShouldForceFastStorageUpdate_FastVersion_Greater_True(t *testing.T) {
	db := db.NewMemDB()
	ndb := newNodeDB(db, 0, nil)
	ndb.latestVersion = 100
	ndb.storageVersion = fastStorageVersionValue + fastStorageVersionDelimiter + strconv.Itoa(int(ndb.latestVersion+1))

	require.True(t, ndb.shouldForceFastStorageUpgrade())
}

func TestShouldForceFastStorageUpdate_FastVersion_Smaller_True(t *testing.T) {
	db := db.NewMemDB()
	ndb := newNodeDB(db, 0, nil)
	ndb.latestVersion = 100
	ndb.storageVersion = fastStorageVersionValue + fastStorageVersionDelimiter + strconv.Itoa(int(ndb.latestVersion-1))

	require.True(t, ndb.shouldForceFastStorageUpgrade())
}

func TestShouldForceFastStorageUpdate_FastVersion_Match_False(t *testing.T) {
	db := db.NewMemDB()
	ndb := newNodeDB(db, 0, nil)
	ndb.latestVersion = 100
	ndb.storageVersion = fastStorageVersionValue + fastStorageVersionDelimiter + strconv.Itoa(int(ndb.latestVersion))

	require.False(t, ndb.shouldForceFastStorageUpgrade())
}

func TestIsFastStorageEnabled_True(t *testing.T) {
	db := db.NewMemDB()
	ndb := newNodeDB(db, 0, nil)
	ndb.latestVersion = 100
	ndb.storageVersion = fastStorageVersionValue + fastStorageVersionDelimiter + strconv.Itoa(int(ndb.latestVersion))

	require.True(t, ndb.hasUpgradedToFastStorage())
}

func TestIsFastStorageEnabled_False(t *testing.T) {
	db := db.NewMemDB()
	ndb := newNodeDB(db, 0, nil)
	ndb.latestVersion = 100
	ndb.storageVersion = defaultStorageVersionValue

	require.False(t, ndb.shouldForceFastStorageUpgrade())
}

func makeHashes(b *testing.B, seed int64) [][]byte {
	b.StopTimer()
	rnd := rand.NewSource(seed)
	hashes := make([][]byte, b.N)
	hashBytes := 8 * ((hashSize + 7) / 8)
	for i := 0; i < b.N; i++ {
		hashes[i] = make([]byte, hashBytes)
		for b := 0; b < hashBytes; b += 8 {
			binary.BigEndian.PutUint64(hashes[i][b:b+8], uint64(rnd.Int63()))
		}
		hashes[i] = hashes[i][:hashSize]
	}
	b.StartTimer()
	return hashes
}

func TestNodeDB_SaveFastNode_ReturnsErrorForNilKey(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockNode := NewMockNode(ctrl)
	mockNode.EXPECT().getKey().Return(nil).Times(1)

	ndb, _, _, _ := createNodeDbWithMocks(ctrl)
	err := ndb.SaveFastNode(mockNode)
	require.Error(t, err)
}

func TestNodeDB_SaveFastNode_FailsWhenUnableToWriteToBuffer(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockNode := NewMockNode(ctrl)
	var buf bytes.Buffer
	mockNode.EXPECT().getKey().Return([]byte("k1")).Times(1)
	mockNode.EXPECT().EncodedSize().Return(1).Times(1)
	mockNode.EXPECT().WriteBytes(gomock.AssignableToTypeOf(&buf)).Return(fmt.Errorf("error")).Times(1)

	ndb, _, _, _ := createNodeDbWithMocks(ctrl)
	err := ndb.SaveFastNode(mockNode)
	require.Error(t, err)
}

// TODO: We should have explicit argument types for below functions
func TestNodeDB_SaveFastNode_ReturnErrorWhenNdbBatchFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockNode := NewMockNode(ctrl)
	ndb, _, mockBatch, _ := createNodeDbWithMocks(ctrl)

	var buf bytes.Buffer
	buf.Grow(40)

	mockNode.EXPECT().getKey().Return([]byte("k1")).Times(2)
	mockNode.EXPECT().EncodedSize().Return(1).Times(1)
	mockNode.EXPECT().WriteBytes(gomock.AssignableToTypeOf(&buf)).
		Return(nil).
		Do(func(buf *bytes.Buffer) {
			buf.Write([]byte("test"))
		}).Times(1)

	mockBatch.EXPECT().Set(gomock.Any(), gomock.Any()).Return(fmt.Errorf("error")).Times(1)

	err := ndb.SaveFastNode(mockNode)
	require.Error(t, err)
}

func TestNodeDB_SaveFastNode(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockNode := NewMockNode(ctrl)
	ndb, _, mockBatch, _ := createNodeDbWithMocks(ctrl)

	var buf bytes.Buffer
	buf.Grow(40)

	mockNode.EXPECT().getKey().Return([]byte("k1")).Times(4)
	mockNode.EXPECT().EncodedSize().Return(1).Times(1)
	mockNode.EXPECT().WriteBytes(gomock.AssignableToTypeOf(&buf)).Return(nil).
		Do(func(buf *bytes.Buffer) {
			buf.Write([]byte("test"))
		}).Times(1)

	mockBatch.EXPECT().Set(gomock.Any(), gomock.Any()).Return(nil).Times(1)

	err := ndb.SaveFastNode(mockNode)
	require.NoError(t, err)
}

func TestNodeDB_GetNodeFailsWhenNil(t *testing.T) {
	ctrl := gomock.NewController(t)
	ndb, dbMock, _, getConstructorCall := createNodeDbWithMocks(ctrl)
	require.Panics(t, func() {
		ndb.GetNode(nil)
	})
	gomock.InOrder(
		getConstructorCall,
		dbMock.EXPECT().Get(gomock.Any()).Return(nil, nil),
	)

	require.Panics(t, func() {
		ndb.GetNode([]byte("k1"))
	})

}

func TestNodeDB_SaveNode(t *testing.T) {
	ctrl := gomock.NewController(t)
	ndb, _, mockBatch, _ := createNodeDbWithMocks(ctrl)
	nilNode := NewMockComplexNode(ctrl)
	nilNode.EXPECT().getHash().Return(nil).Times(1)
	require.Panics(t, func() {
		ndb.SaveNode(nilNode)
	})

	alreadyPersistedNode := NewMockComplexNode(ctrl)
	alreadyPersistedNode.EXPECT().getHash().Return([]byte("Fred")).Times(1)
	alreadyPersistedNode.EXPECT().Persisted().Return(true).Times(1)
	require.Panics(t, func() {
		ndb.SaveNode(alreadyPersistedNode)
	})

	happyNode := NewMockComplexNode(ctrl)
	happyNode.EXPECT().getHash().Return([]byte("Fred")).Times(5)
	happyNode.EXPECT().Persisted().Return(false).Times(1)
	happyNode.EXPECT().EncodedSize().Return(10).Times(1)
	happyNode.EXPECT().WriteBytes(gomock.Any()).Return(nil).Times(1)
	happyNode.EXPECT().setPersisted(true).Times(1)

	mockBatch.EXPECT().Set(gomock.Any(), gomock.Any()).Return(nil)

	ndb.SaveNode(happyNode)

}

func createNodeDbWithMocks(ctrl *gomock.Controller) (*nodeDB, *mock.MockDB, *mock.MockBatch, *gomock.Call) {
	const expectedVersion = defaultStorageVersionValue

	dbMock := mock.NewMockDB(ctrl)
	mockBatch := mock.NewMockBatch(ctrl)

	getFromDbCall := dbMock.EXPECT().Get(gomock.Any()).Return([]byte(expectedVersion), nil)
	dbMock.EXPECT().NewBatch().Return(mockBatch).Times(1)

	return newNodeDB(dbMock, 0, nil), dbMock, mockBatch, getFromDbCall
}
