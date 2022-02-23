package iavl

import (
	"bytes"
	"encoding/hex"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNode_EncodedSize(t *testing.T) {
	node := &TreeNode{
		key:       randBytes(10),
		value:     randBytes(10),
		version:   1,
		height:    0,
		size:      100,
		hash:      randBytes(20),
		leftHash:  randBytes(20),
		leftNode:  nil,
		rightHash: randBytes(20),
		rightNode: nil,
		persisted: false,
	}

	// leaf node
	require.Equal(t, 26, node.EncodedSize())

	// non-leaf node
	node.height = 1
	require.Equal(t, 57, node.EncodedSize())
}

func TestNode_encode_decode(t *testing.T) {
	testcases := map[string]struct {
		node        *TreeNode
		expectHex   string
		expectError bool
	}{
		"nil":   {nil, "", true},
		"empty": {&TreeNode{}, "0000000000", false},
		"inner": {&TreeNode{
			height:    3,
			version:   2,
			size:      7,
			key:       []byte("key"),
			leftHash:  []byte{0x70, 0x80, 0x90, 0xa0},
			rightHash: []byte{0x10, 0x20, 0x30, 0x40},
		}, "060e04036b657904708090a00410203040", false},
		"leaf": {&TreeNode{
			height:  0,
			version: 3,
			size:    1,
			key:     []byte("key"),
			value:   []byte("value"),
		}, "000206036b65790576616c7565", false},
	}
	for name, tc := range testcases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			err := tc.node.WriteBytes(&buf)
			if tc.expectError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expectHex, hex.EncodeToString(buf.Bytes()))

			node, err := MakeNode(buf.Bytes())
			require.NoError(t, err)
			// since key and value is always decoded to []byte{} we augment the expected struct here
			if tc.node.key == nil {
				tc.node.key = []byte{}
			}
			if tc.node.value == nil && tc.node.height == 0 {
				tc.node.value = []byte{}
			}
			require.Equal(t, tc.node, node)
		})
	}
}

func TestNode_validate(t *testing.T) {
	k := []byte("key")
	v := []byte("value")
	h := []byte{1, 2, 3}
	c := &TreeNode{key: []byte("child"), value: []byte("x"), version: 1, size: 1}

	testcases := map[string]struct {
		node  *TreeNode
		valid bool
	}{
		"nil node":               {nil, false},
		"leaf":                   {&TreeNode{key: k, value: v, version: 1, size: 1}, true},
		"leaf with nil key":      {&TreeNode{key: nil, value: v, version: 1, size: 1}, false},
		"leaf with empty key":    {&TreeNode{key: []byte{}, value: v, version: 1, size: 1}, true},
		"leaf with nil value":    {&TreeNode{key: k, value: nil, version: 1, size: 1}, false},
		"leaf with empty value":  {&TreeNode{key: k, value: []byte{}, version: 1, size: 1}, true},
		"leaf with version 0":    {&TreeNode{key: k, value: v, version: 0, size: 1}, false},
		"leaf with version -1":   {&TreeNode{key: k, value: v, version: -1, size: 1}, false},
		"leaf with size 0":       {&TreeNode{key: k, value: v, version: 1, size: 0}, false},
		"leaf with size 2":       {&TreeNode{key: k, value: v, version: 1, size: 2}, false},
		"leaf with size -1":      {&TreeNode{key: k, value: v, version: 1, size: -1}, false},
		"leaf with left hash":    {&TreeNode{key: k, value: v, version: 1, size: 1, leftHash: h}, false},
		"leaf with left child":   {&TreeNode{key: k, value: v, version: 1, size: 1, leftNode: c}, false},
		"leaf with right hash":   {&TreeNode{key: k, value: v, version: 1, size: 1, rightNode: c}, false},
		"leaf with right child":  {&TreeNode{key: k, value: v, version: 1, size: 1, rightNode: c}, false},
		"inner":                  {&TreeNode{key: k, version: 1, size: 1, height: 1, leftHash: h, rightHash: h}, true},
		"inner with nil key":     {&TreeNode{key: nil, value: v, version: 1, size: 1, height: 1, leftHash: h, rightHash: h}, false},
		"inner with value":       {&TreeNode{key: k, value: v, version: 1, size: 1, height: 1, leftHash: h, rightHash: h}, false},
		"inner with empty value": {&TreeNode{key: k, value: []byte{}, version: 1, size: 1, height: 1, leftHash: h, rightHash: h}, false},
		"inner with left child":  {&TreeNode{key: k, version: 1, size: 1, height: 1, leftHash: h}, true},
		"inner with right child": {&TreeNode{key: k, version: 1, size: 1, height: 1, rightHash: h}, true},
		"inner with no child":    {&TreeNode{key: k, version: 1, size: 1, height: 1}, false},
		"inner with height 0":    {&TreeNode{key: k, version: 1, size: 1, height: 0, leftHash: h, rightHash: h}, false},
	}

	for desc, tc := range testcases {
		tc := tc // appease scopelint
		t.Run(desc, func(t *testing.T) {
			err := tc.node.validate()
			if tc.valid {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func BenchmarkNode_EncodedSize(b *testing.B) {
	node := &TreeNode{
		key:       randBytes(25),
		value:     randBytes(100),
		version:   rand.Int63n(10000000),
		height:    1,
		size:      rand.Int63n(10000000),
		leftHash:  randBytes(20),
		rightHash: randBytes(20),
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		node.EncodedSize()
	}
}

func BenchmarkNode_WriteBytes(b *testing.B) {
	node := &TreeNode{
		key:       randBytes(25),
		value:     randBytes(100),
		version:   rand.Int63n(10000000),
		height:    1,
		size:      rand.Int63n(10000000),
		leftHash:  randBytes(20),
		rightHash: randBytes(20),
	}
	b.ResetTimer()
	b.Run("NoPreAllocate", func(sub *testing.B) {
		sub.ReportAllocs()
		for i := 0; i < sub.N; i++ {
			var buf bytes.Buffer
			buf.Reset()
			_ = node.WriteBytes(&buf)
		}
	})
	b.Run("PreAllocate", func(sub *testing.B) {
		sub.ReportAllocs()
		for i := 0; i < sub.N; i++ {
			var buf bytes.Buffer
			buf.Grow(node.EncodedSize())
			_ = node.WriteBytes(&buf)
		}
	})
}
