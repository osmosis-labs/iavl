package common

import "math/bits"

const (
	UintSizeBytes = bits.UintSize / 8 // 4 or 8

	Uint64Size = 8
)

func GetSliceSizeBytes() int {
	return 3 * UintSizeBytes // len + capacity + pointer to underlying array
}
