package utils

import dbm "github.com/tendermint/tm-db"

// Traverse all keys with a certain prefix. Return error if any, nil otherwise
func IterateThroughAllSubtreeKeys(db dbm.DB, prefix []byte, fn func(k, v []byte) error) error {
	itr, err := dbm.IteratePrefix(db, prefix)
	if err != nil {
		return err
	}
	defer itr.Close()

	for ; itr.Valid(); itr.Next() {
		if err := fn(itr.Key(), itr.Value()); err != nil {
			return err
		}
	}

	return nil
}

func GatherSubtreeKVPairs(db dbm.DB, prefix []byte) (keys [][]byte, values [][]byte, err error) {
	gatherFn := func(k, v []byte) error {
		keys = append(keys, k)
		values = append(values, v)
		return nil
	}

	err = IterateThroughAllSubtreeKeys(db, prefix, gatherFn)
	return keys, values, err
}
