package iavl

import (
	"encoding/binary"
	"errors"
	"math/rand"
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

func TestNewNoDbChain_ChainVersionInDb_Success(t *testing.T) {
	const expectedVersion = "2"
	
	ctrl := gomock.NewController(t)
	dbMock := mock.NewMockDB(ctrl)

	dbMock.EXPECT().Get(gomock.Any()).Return([]byte(expectedVersion), nil).Times(1)
	dbMock.EXPECT().NewBatch().Return(nil).Times(1)
	
	ndb := newNodeDB(dbMock, 0, nil)
	require.Equal(t, expectedVersion, ndb.chainVersion)
}

func TestNewNoDbChain_ErrorInConstructor_DefaultSet(t *testing.T) {
	const expectedVersion = defaultChainVersionValue
	
	ctrl := gomock.NewController(t)
	dbMock := mock.NewMockDB(ctrl)

	dbMock.EXPECT().Get(gomock.Any()).Return(nil, errors.New("some db error")).Times(1)
	dbMock.EXPECT().NewBatch().Return(nil).Times(1)
	
	ndb := newNodeDB(dbMock, 0, nil)
	require.Equal(t, expectedVersion, string(ndb.getChainVersion()))
}

func TestNewNoDbChain_DoesNotExist_DefaultSet(t *testing.T) {
	const expectedVersion = defaultChainVersionValue
	
	ctrl := gomock.NewController(t)
	dbMock := mock.NewMockDB(ctrl)

	dbMock.EXPECT().Get(gomock.Any()).Return(nil, nil).Times(1)
	dbMock.EXPECT().NewBatch().Return(nil).Times(1)
	
	ndb := newNodeDB(dbMock, 0, nil)
	require.Equal(t, expectedVersion, string(ndb.getChainVersion()))
}

func TestSetChainVersion_Success(t *testing.T) {
	const expectedVersion = "2"
	
	db := db.NewMemDB()
	
	ndb := newNodeDB(db, 0, nil)
	require.Equal(t, defaultChainVersionValue, string(ndb.getChainVersion()))

	err := ndb.setChainVersion([]byte(expectedVersion))
	require.NoError(t, err)
	require.Equal(t, expectedVersion, string(ndb.getChainVersion()))
}

func TestSetChainVersion_Failure_OldKept(t *testing.T) {
	ctrl := gomock.NewController(t)
	
	dbMock := mock.NewMockDB(ctrl)
	dbMock.EXPECT().Get(gomock.Any()).Return([]byte(defaultChainVersionValue), nil).Times(1)
	dbMock.EXPECT().NewBatch().Return(nil).Times(1)
	dbMock.EXPECT().Set(gomock.Any(), gomock.Any()).Return(errors.New("some db error")).Times(1)

	ndb := newNodeDB(dbMock, 0, nil)
	require.Equal(t, defaultChainVersionValue, string(ndb.getChainVersion()))

	ndb.setChainVersion([]byte("2"))
	require.Equal(t, defaultChainVersionValue, string(ndb.getChainVersion()))
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
