package iavl

import dbm "github.com/tendermint/tm-db"

// Iterator is a dbm.Iterator for ImmutableTree
type FastIterator struct {
	start, end []byte

	valid bool

	ascending bool

	err error

	ndb *nodeDB

	nextFastNode *FastNode

	fastIterator dbm.Iterator
}

var _ dbm.Iterator = &FastIterator{}

func NewFastIterator(start, end []byte, ascending bool, ndb *nodeDB) *FastIterator {
	iter := &FastIterator{
		start:       start,
		end:         end,
		err:         nil,
		ascending:   ascending,
		ndb:         ndb,
		nextFastNode: nil,
		fastIterator: nil,
	}
	iter.Next()
	return iter
}

// Domain implements dbm.Iterator.
func (iter *FastIterator) Domain() ([]byte, []byte) {
	start, end := iter.fastIterator.Domain()

	if start != nil {
		start = start[1:]
	}

	if end != nil {
		end = end[1:]
	}

	return start, end
}

// Valid implements dbm.Iterator.
func (iter *FastIterator) Valid() bool {
	if iter.fastIterator == nil || !iter.fastIterator.Valid() {
		return false
	}

	return iter.valid
}

// Key implements dbm.Iterator
func (iter *FastIterator) Key() []byte {
	if iter.valid {
		return iter.nextFastNode.key
	}
	return nil
}

// Value implements dbm.Iterator
func (iter *FastIterator) Value() []byte {
	if iter.valid {
		return iter.nextFastNode.value
	}
	return nil
}

// Next implements dbm.Iterator
func (iter *FastIterator) Next() {
	if iter.fastIterator == nil {
		iter.fastIterator, iter.err = iter.ndb.getFastIterator(iter.start, iter.end, iter.ascending)
		iter.valid = iter.err == nil

	} else {
		iter.fastIterator.Next()
		iter.err = iter.fastIterator.Error()
	}

	iter.valid = iter.valid && iter.fastIterator.Valid()
	if iter.valid {
		iter.nextFastNode, iter.err = DeserializeFastNode(iter.fastIterator.Key()[1:], iter.fastIterator.Value())
		iter.valid = iter.err == nil
	}
}

// Close implements dbm.Iterator
func (iter *FastIterator) Close() error {
	iter.fastIterator = nil
	iter.valid = false
	iter.err = iter.fastIterator.Close()
	return iter.err
}

// Error implements dbm.Iterator
func (iter *FastIterator) Error() error {
	return iter.err
}

// IsFast returnts true if iterator uses fast strategy
func (iter *FastIterator) IsFast() bool {
	return true
}
