package cache

var (
	GetCacheElemMetadataSize = getCacheElemMetadataSize
	GetCacheCurrentBytes     = getCacheCurrentBytes
)

// Used for testing, returns removed Nodes
func MockAdd(c Cache, node Node) []Node {
	c.add(node)

	removed := make([]Node, 0)
	for c.isOverLimit() {
		removed = append(removed, c.remove(c.getOldest()))
	}
	return removed
}
