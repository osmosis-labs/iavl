package iavl

import (
	"bytes"
	"errors"
	"sort"

	dbm "github.com/tendermint/tm-db"
)

var (
	errUnsavedFastIteratorNilAdditionsGiven = errors.New("unsaved fast iterator must be created with unsaved additions but they were nil")

	errUnsavedFastIteratorNilRemovalsGiven = errors.New("unsaved fast iterator must be created with unsaved removals but they were nil")
)

// UnsavedFastIterator is a dbm.Iterator for ImmutableTree
// it iterates over the latest state via fast nodes,
// taking advantage of keys being located in sequence in the underlying database.
type UnsavedFastIterator struct {
	start, end []byte

	valid bool

	ascending bool

	err error

	ndb *nodeDB

	unsavedFastNodeAdditions map[string]*FastNode

	unsavedFastNodeRemovals map[string]interface{}

	unsavedFastNodesToSort []string

	nextKey []byte

	nextVal []byte

	nextUnsavedNodeIdx int

	fastIterator dbm.Iterator
}

var _ dbm.Iterator = &UnsavedFastIterator{}

func NewUnsavedFastIterator(start, end []byte, ascending bool, ndb *nodeDB, unsavedFastNodeAdditions map[string]*FastNode, unsavedFastNodeRemovals map[string]interface{}) *UnsavedFastIterator {

	iter := &UnsavedFastIterator{
		start:                    start,
		end:                      end,
		err:                      nil,
		ascending:                ascending,
		ndb:                      ndb,
		unsavedFastNodeAdditions: unsavedFastNodeAdditions,
		unsavedFastNodeRemovals:  unsavedFastNodeRemovals,
		unsavedFastNodesToSort:   make([]string, 0),
		nextKey:                  nil,
		nextVal:                  nil,
		nextUnsavedNodeIdx:       0,
		fastIterator:             NewFastIterator(start, end, ascending, ndb),
	}

	// We need to ensure that we iterate over saved and unsaved state in order.
	// The strategy is to sort unsaved nodes, the fast node on disk are already sorted.
	// Then, we keep a pointer to both the unsaved and saved nodes, and iterate over them in order efficiently.
	for _, fastNode := range unsavedFastNodeAdditions {
		if start != nil && bytes.Compare(fastNode.key, start) < 0 {
			continue
		}

		if end != nil && bytes.Compare(fastNode.key, end) >= 0 {
			continue
		}

		iter.unsavedFastNodesToSort = append(iter.unsavedFastNodesToSort, string(fastNode.key))
	}

	sort.Slice(iter.unsavedFastNodesToSort, func(i, j int) bool {
		if ascending {
			return iter.unsavedFastNodesToSort[i] < iter.unsavedFastNodesToSort[j]
		} else {
			return iter.unsavedFastNodesToSort[i] > iter.unsavedFastNodesToSort[j]
		}
	})

	if iter.ndb == nil {
		iter.err = errFastIteratorNilNdbGiven
		iter.valid = false
		return iter
	}

	if iter.unsavedFastNodeAdditions == nil {
		iter.err = errUnsavedFastIteratorNilAdditionsGiven
		iter.valid = false
		return iter
	}

	if iter.unsavedFastNodeRemovals == nil {
		iter.err = errUnsavedFastIteratorNilRemovalsGiven
		iter.valid = false
		return iter
	}

	// Move to the first elemenet
	iter.Next()

	return iter
}

// Domain implements dbm.Iterator.
// Maps the underlying nodedb iterator domain, to the 'logical' keys involved.
func (iter *UnsavedFastIterator) Domain() ([]byte, []byte) {
	return iter.start, iter.end
}

// Valid implements dbm.Iterator.
func (iter *UnsavedFastIterator) Valid() bool {
	if iter.start != nil && iter.end != nil {
		if bytes.Compare(iter.end, iter.start) != 1 {
			return false
		}
	}

	return iter.fastIterator.Valid() || iter.nextUnsavedNodeIdx < len(iter.unsavedFastNodesToSort) || (iter.nextKey != nil && iter.nextVal != nil)
}

// Key implements dbm.Iterator
func (iter *UnsavedFastIterator) Key() []byte {
	return iter.nextKey
}

// Value implements dbm.Iterator
func (iter *UnsavedFastIterator) Value() []byte {
	return iter.nextVal
}

// Next implements dbm.Iterator
// Its effectively running the constant space overhead algorithm for streaming through sorted lists:
// the sorted lists being underlying fast nodes & unsavedFastNodeChanges
func (iter *UnsavedFastIterator) Next() {
	if iter.ndb == nil {
		iter.err = errFastIteratorNilNdbGiven
		iter.valid = false
		return
	}

	if iter.fastIterator.Valid() && iter.nextUnsavedNodeIdx < len(iter.unsavedFastNodesToSort) {
		diskKeyStr := string(iter.fastIterator.Key())

		if iter.unsavedFastNodeRemovals[diskKeyStr] != nil {
			// If next fast node from disk is to be removed, skip it.
			iter.fastIterator.Next()
			iter.Next()
			return
		}

		nextUnsavedKey := iter.unsavedFastNodesToSort[iter.nextUnsavedNodeIdx]
		nextUnsavedNode := iter.unsavedFastNodeAdditions[nextUnsavedKey]

		var isUnsavedNext bool
		if iter.ascending {
			isUnsavedNext = diskKeyStr >= nextUnsavedKey
		} else {
			isUnsavedNext = diskKeyStr <= nextUnsavedKey
		}

		if isUnsavedNext {
			// Unsaved node is next

			if diskKeyStr == nextUnsavedKey {
				// Unsaved update prevails over saved copy so we skip the copy from disk
				iter.fastIterator.Next()
			}

			iter.nextKey = nextUnsavedNode.key
			iter.nextVal = nextUnsavedNode.value

			iter.nextUnsavedNodeIdx++
			return
		} else {
			// Disk node is next
			iter.nextKey = iter.fastIterator.Key()
			iter.nextVal = iter.fastIterator.Value()

			iter.fastIterator.Next()
			return
		}
	}

	// if only nodes on disk are left, we return them
	if iter.fastIterator.Valid() {
		if iter.unsavedFastNodeRemovals[string(iter.fastIterator.Key())] != nil {
			// If next fast node from disk is to be removed, skip it.
			iter.fastIterator.Next()
			iter.Next()
			return
		}

		iter.nextKey = iter.fastIterator.Key()
		iter.nextVal = iter.fastIterator.Value()

		iter.fastIterator.Next()
		return
	}

	// if only unsaved nodes are left, we can just iterate
	if iter.nextUnsavedNodeIdx < len(iter.unsavedFastNodesToSort) {
		nextUnsavedKey := iter.unsavedFastNodesToSort[iter.nextUnsavedNodeIdx]
		nextUnsavedNode := iter.unsavedFastNodeAdditions[nextUnsavedKey]

		iter.nextKey = nextUnsavedNode.key
		iter.nextVal = nextUnsavedNode.value

		iter.nextUnsavedNodeIdx++
		return
	}

	iter.nextKey = nil
	iter.nextVal = nil
}

// Close implements dbm.Iterator
func (iter *UnsavedFastIterator) Close() error {
	iter.valid = false
	return iter.fastIterator.Close()
}

// Error implements dbm.Iterator
func (iter *UnsavedFastIterator) Error() error {
	return iter.err
}
