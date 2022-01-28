package iavl

import (
	"encoding/binary"
	"errors"
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
	dbMock := mock.NewMockDB(ctrl)

	dbMock.EXPECT().Get(gomock.Any()).Return([]byte(expectedVersion), nil).Times(1)
	dbMock.EXPECT().NewBatch().Return(nil).Times(1)

	ndb := newNodeDB(dbMock, 0, nil)
	require.Equal(t, expectedVersion, ndb.storageVersion)
}

func TestNewNoDbStorage_ErrorInConstructor_DefaultSet(t *testing.T) {
	const expectedVersion = defaultStorageVersionValue

	ctrl := gomock.NewController(t)
	dbMock := mock.NewMockDB(ctrl)

	dbMock.EXPECT().Get(gomock.Any()).Return(nil, errors.New("some db error")).Times(1)
	dbMock.EXPECT().NewBatch().Return(nil).Times(1)

	ndb := newNodeDB(dbMock, 0, nil)
	require.Equal(t, expectedVersion, string(ndb.getStorageVersion()))
}

func TestNewNoDbStorage_DoesNotExist_DefaultSet(t *testing.T) {
	const expectedVersion = defaultStorageVersionValue

	ctrl := gomock.NewController(t)
	dbMock := mock.NewMockDB(ctrl)

	dbMock.EXPECT().Get(gomock.Any()).Return(nil, nil).Times(1)
	dbMock.EXPECT().NewBatch().Return(nil).Times(1)

	ndb := newNodeDB(dbMock, 0, nil)
	require.Equal(t, expectedVersion, string(ndb.getStorageVersion()))
}

func TestSetStorageVersion_Success(t *testing.T) {
	const expectedVersion = fastStorageVersionValue

	db := db.NewMemDB()

	ndb := newNodeDB(db, 0, nil)
	require.Equal(t, defaultStorageVersionValue, string(ndb.getStorageVersion()))

	err := ndb.setStorageVersion(expectedVersion)
	require.NoError(t, err)
	require.Equal(t, expectedVersion + fastStorageVersionDelimiter + strconv.Itoa(int(ndb.getLatestVersion())), string(ndb.getStorageVersion()))
}

func TestSetStorageVersion_Failure_OldKept(t *testing.T) {
	ctrl := gomock.NewController(t)

	dbMock := mock.NewMockDB(ctrl)
	dbMock.EXPECT().Get(gomock.Any()).Return([]byte(defaultStorageVersionValue), nil).Times(1)
	dbMock.EXPECT().NewBatch().Return(nil).Times(1)
	dbMock.EXPECT().Set(gomock.Any(), gomock.Any()).Return(errors.New("some db error")).Times(1)

	ndb := newNodeDB(dbMock, 0, nil)
	require.Equal(t, defaultStorageVersionValue, string(ndb.getStorageVersion()))

	ndb.setStorageVersion(fastStorageVersionValue)
	require.Equal(t, defaultStorageVersionValue, string(ndb.getStorageVersion()))
}

func TestSetStorageVersion_FastVersionFirst_VersionAppended(t *testing.T) {
	db := db.NewMemDB()
	ndb := newNodeDB(db, 0, nil)
	ndb.storageVersion = fastStorageVersionValue
	ndb.latestVersion = 100

	err := ndb.setStorageVersion(fastStorageVersionValue)
	require.NoError(t, err)
	require.Equal(t, fastStorageVersionValue + fastStorageVersionDelimiter + strconv.Itoa(int(ndb.latestVersion)), ndb.storageVersion)
}

func TestSetStorageVersion_FastVersionSecond_VersionAppended(t *testing.T) {
	db := db.NewMemDB()
	ndb := newNodeDB(db, 0, nil)
	ndb.storageVersion = fastStorageVersionValue
	ndb.latestVersion = 100

	storageVersionBytes := []byte(fastStorageVersionValue)
	storageVersionBytes[len(fastStorageVersionValue) - 1]++ // increment last byte

	err := ndb.setStorageVersion(string(storageVersionBytes))
	require.NoError(t, err)
	require.Equal(t, string(storageVersionBytes) + fastStorageVersionDelimiter + strconv.Itoa(int(ndb.latestVersion)), ndb.storageVersion)
}

func TestShouldForceFastStorageUpdate_DefaultVersion_True(t *testing.T) {
	db := db.NewMemDB()
	ndb := newNodeDB(db, 0, nil)
	ndb.storageVersion = defaultStorageVersionValue
	ndb.latestVersion = 100

	require.False(t, ndb.shouldForceFastStorageUpdate())
}

func TestShouldForceFastStorageUpdate_FastVersion_Greater_True(t *testing.T) {
	db := db.NewMemDB()
	ndb := newNodeDB(db, 0, nil)
	ndb.latestVersion = 100
	ndb.storageVersion = fastStorageVersionValue + fastStorageVersionDelimiter + strconv.Itoa(int(ndb.latestVersion + 1))

	require.True(t, ndb.shouldForceFastStorageUpdate())
}

func TestShouldForceFastStorageUpdate_FastVersion_Smaller_True(t *testing.T) {
	db := db.NewMemDB()
	ndb := newNodeDB(db, 0, nil)
	ndb.latestVersion = 100
	ndb.storageVersion = fastStorageVersionValue + fastStorageVersionDelimiter + strconv.Itoa(int(ndb.latestVersion - 1))

	require.True(t, ndb.shouldForceFastStorageUpdate())
}

func TestShouldForceFastStorageUpdate_FastVersion_Match_False(t *testing.T) {
	db := db.NewMemDB()
	ndb := newNodeDB(db, 0, nil)
	ndb.latestVersion = 100
	ndb.storageVersion = fastStorageVersionValue + fastStorageVersionDelimiter + strconv.Itoa(int(ndb.latestVersion))

	require.False(t, ndb.shouldForceFastStorageUpdate())
}

func TestIsFastStorageEnabled_True(t *testing.T) {
	db := db.NewMemDB()
	ndb := newNodeDB(db, 0, nil)
	ndb.latestVersion = 100
	ndb.storageVersion = fastStorageVersionValue + fastStorageVersionDelimiter + strconv.Itoa(int(ndb.latestVersion))

	require.True(t, ndb.isFastStorageEnabled())
}

func TestIsFastStorageEnabled_False(t *testing.T) {
	db := db.NewMemDB()
	ndb := newNodeDB(db, 0, nil)
	ndb.latestVersion = 100
	ndb.storageVersion = defaultStorageVersionValue

	require.False(t, ndb.shouldForceFastStorageUpdate())
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
