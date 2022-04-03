package cache

var (
	GetCacheElemMetadataSize = getCacheElemMetadataSize
	GetCacheCurrentBytes     = getCacheCurrentBytes
)

// Used for testing, returns removed Nodes
func MockAdd(c Cache, node Node) []Node {
	if old := c.add(node); old != nil {
		return []Node{old}
	}

	removed := make([]Node, 0)
	for c.isOverLimit() {
		removed = append(removed, c.remove(c.getOldest()))
	}
	return removed
}
