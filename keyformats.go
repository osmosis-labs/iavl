package iavl

import "github.com/cosmos/iavl/keyformat"

var (
	// All node keys are prefixed with the byte 'n'. This ensures no collision is
	// possible with the other keys, and makes them easier to traverse. They are indexed by the node hash.
	nodeKeyFormat = keyformat.NewKeyFormat('n', hashSize) // n<hash>

	// Orphans are keyed in the database by their expected lifetime.
	// The first number represents the *last* version at which the orphan needs
	// to exist, while the second number represents the *earliest* version at
	// which it is expected to exist - which starts out by being the version
	// of the node being orphaned.
	// To clarify:
	// When I write to key {X} with value V and old value O, we orphan O with <last-version>=time of write
	// and <first-version> = version O was created at.
	orphanKeyFormat = keyformat.NewKeyFormat('o', int64Size, int64Size, hashSize) // o<last-version><first-version><hash>

	// Key Format for making reads and iterates go through a data-locality preserving db.
	// The value at an entry will list what version it was written to.
	// Then to query values, you first query state via this fast method.
	// If its present, then check the tree version. If tree version >= result_version,
	// return result_version. Else, go through old (slow) IAVL get method that walks through tree.
	fastKeyFormat = keyformat.NewKeyFormat('f', 0) // f<keystring>

	// Key Format for storing metadata about the chain such as the vesion number.
	// The value at an entry will be in a variable format and up to the caller to
	// decide how to parse.
	metadataKeyFormat = keyformat.NewKeyFormat('m', 0) // v<keystring>

	// Root nodes are indexed separately by their version
	rootKeyFormat = keyformat.NewKeyFormat('r', int64Size) // r<version>
)

func getRootKey(version int64) []byte {
	return rootKeyFormat.Key(version)
}

func getNodeKey(hash []byte) []byte {
	return nodeKeyFormat.KeyBytes(hash)
}

func getFastNodeKey(key []byte) []byte {
	return fastKeyFormat.KeyBytes(key)
}

func getOrphanKey(fromVersion, toVersion int64, hash []byte) []byte {
	return orphanKeyFormat.Key(toVersion, fromVersion, hash)
}
