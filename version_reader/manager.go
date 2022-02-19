package versionreader

import (
	"sync"

	"github.com/cosmos/iavl/logger"
)


type  Manager struct {
	versionReadersLock 	*sync.Mutex 	// Protects versionReaders
	versionReaders map[int64]Waiter // Number of active version readers
}

func NewManager() *Manager {
	return &Manager{
		versionReadersLock: &sync.Mutex{},
		versionReaders: make(map[int64]Waiter),
	}
}

func (vrm *Manager) Wait(version int64) {
	vrm.versionReadersLock.Lock()
	vr, ok := vrm.versionReaders[version]
	vrm.versionReadersLock.Unlock()

	if ok {
		vr.Wait()
	}
}

func (vrm *Manager) Increment(version int64) {
	logger.Debug("---->Incrementing version: %d\n", version)
	vrm.versionReadersLock.Lock()
	defer vrm.versionReadersLock.Unlock()

	vr, ok := vrm.versionReaders[version]

	if !ok {
		vr = NewReader()
		vrm.versionReaders[version] = vr
	}

	logger.Debug("vr.Add version: %d\n", version)
	vr.Add()
	logger.Debug("<----Incrementing version: %d\n", version)
}

func (vrm *Manager) Decrement(version int64) {
	logger.Debug("---->Decrementing version: %d\n", version)
	vrm.versionReadersLock.Lock()
	defer vrm.versionReadersLock.Unlock()

	if vr, ok := vrm.versionReaders[version]; ok {
		logger.Debug("vr.Done version: %d\n", version)
		vr.Done()
	}

	logger.Debug("<----Decrementing version: %d\n", version)
}
