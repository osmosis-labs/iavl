package versionreader_test

import (
	"context"
	"sync"
	"testing"
	"time"

	versionreader "github.com/cosmos/iavl/version_reader"

	"github.com/stretchr/testify/require"
)

func TestReader_New(t *testing.T) {
	vr := versionreader.NewReader()

	require.NotNil(t, vr)
}

func TestReader_Parallel(t *testing.T) {
	vr := versionreader.NewReader()

	mx := sync.Mutex{}

	for i := 0; i < 100; i++ {
		go func() {
			for j := 0; j < 100000; j++ {
				mx.Lock()
				vr.Add()
				mx.Unlock()

				mx.Lock()
				vr.Done()
				mx.Unlock()
			}
		}()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		for i := 0; i < 100; i++ {
			vr.Wait()
			time.Sleep(time.Millisecond * 10)
		}
		cancel()
	}()

	select {
	case <-time.After(10 * time.Second):
		t.Error("timed out")
	case <-ctx.Done():
		return
	}
}
