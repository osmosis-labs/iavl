package iavl

// This file is responsible for:
// * logic for checking if we should re-index or migrate to fast nodes
// * logic for actually re-indexing / migrating to add fast nodes for live state.

import (
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/cosmos/iavl/types"
)

// Returns true if the upgrade to fast storage has occurred but it does not match the live state, false otherwise.
// When the live state is not matched, we must force reupgrade.
// We determine this by checking the version of the live state and the version of the live state when
// latest storage was updated on disk the last time.
func (ndb *nodeDB) shouldForceFastStorageUpgrade() bool {
	versions := strings.Split(ndb.storageVersion, fastStorageVersionDelimiter)

	if len(versions) == 2 {
		if versions[1] != strconv.Itoa(int(ndb.getLatestVersion())) {
			return true
		}
	}
	return false
}

func (tree *MutableTree) enableFastStorageAndCommit() error {
	debug("enabling fast storage, might take a while.")
	var err error
	defer func() {
		if err != nil {
			debug("failed to enable fast storage: %v\n", err)
		} else {
			debug("fast storage is enabled.")
		}
	}()

	// We start a new thread to keep on checking if we are above 4GB, and if so garbage collect.
	// This thread only lasts during the fast node migration.
	// This is done to keep RAM usage down.
	done := make(chan struct{})
	defer func() {
		done <- struct{}{}
		close(done)
	}()

	go func() {
		timer := time.NewTimer(time.Second)
		var m runtime.MemStats

		for {
			// Sample the current memory usage
			runtime.ReadMemStats(&m)

			if m.Alloc > 4*1024*1024*1024 {
				// If we are using more than 4GB of memory, we should trigger garbage collection
				// to free up some memory.
				runtime.GC()
			}

			select {
			case <-timer.C:
				timer.Reset(time.Second)
			case <-done:
				if !timer.Stop() {
					<-timer.C
				}
				return
			}
		}
	}()

	itr := NewIterator(nil, nil, true, tree.ImmutableTree)
	defer itr.Close()
	for ; itr.Valid(); itr.Next() {
		if err = tree.ndb.SaveFastNodeNoCache(types.NewFastNode(itr.Key(), itr.Value(), tree.version)); err != nil {
			return err
		}
	}

	if err = itr.Error(); err != nil {
		return err
	}

	if err = tree.ndb.setFastStorageVersionToBatch(); err != nil {
		return err
	}

	if err = tree.ndb.Commit(); err != nil {
		return err
	}
	return nil
}

// enableFastStorageAndCommitIfNotEnabled if nodeDB doesn't mark fast storage as enabled, enable it, and commit the update.
// Checks whether the fast cache on disk matches latest live state. If not, deletes all existing fast nodes and repopulates them
// from latest tree.
func (tree *MutableTree) enableFastStorageAndCommitIfNotEnabled() (bool, error) {
	shouldForceUpdate := tree.ndb.shouldForceFastStorageUpgrade()
	isFastStorageEnabled := tree.ndb.hasUpgradedToFastStorage()

	if !tree.IsUpgradeable() {
		return false, nil
	}

	if isFastStorageEnabled && shouldForceUpdate {
		// If there is a mismatch between which fast nodes are on disk and the live state due to temporary
		// downgrade and subsequent re-upgrade, we cannot know for sure which fast nodes have been removed while downgraded,
		// Therefore, there might exist stale fast nodes on disk. As a result, to avoid persisting the stale state, it might
		// be worth to delete the fast nodes from disk.
		fastItr := NewFastIterator(nil, nil, true, tree.ndb)
		defer fastItr.Close()
		for ; fastItr.Valid(); fastItr.Next() {
			if err := tree.ndb.DeleteFastNode(fastItr.Key()); err != nil {
				return false, err
			}
		}
	}

	// Force garbage collection before we proceed to enabling fast storage.
	runtime.GC()

	if err := tree.enableFastStorageAndCommit(); err != nil {
		tree.ndb.storageVersion = defaultStorageVersionValue
		return false, err
	}
	return true, nil
}

func (tree *MutableTree) enableFastStorageAndCommitLocked() error {
	tree.mtx.Lock()
	defer tree.mtx.Unlock()
	return tree.enableFastStorageAndCommit()
}
