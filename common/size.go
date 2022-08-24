package common

import "math/bits"

const (
	UintSizeBytes = bits.UintSize / 8 // 4 or 8

	Uint64Size = 8
)

// GetSliceSizeBytes returns how how much space Go slice utilizes in memory.
// Slices have integers for length and capacity. Additionaly, they contain 
// a pointer to the underlying array buffer.
func GetSliceSizeBytes() int {
	return 3 * UintSizeBytes
}

// GetStringSizeBytes returns how how much space Go string utilizes in memory.
// Strings have an integer for length and a pointer to the underlying array buffer.
// Contrary to slices, strings do not contain capacity as they are immutable.
func GetStringSizeBytes() int {
	return 2 * UintSizeBytes
}
