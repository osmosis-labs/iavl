package versionreader_test

import (
	"context"
	"sync"
	"testing"
	"time"

	versionreader "github.com/cosmos/iavl/version_reader"

	"github.com/stretchr/testify/require"
)

func TestManager_New(t *testing.T) {
	manager := versionreader.NewManager()
	require.NotNil(t, manager)
}

func TestManager_Wait_NoActiveVersion_ShouldNotBlock(t *testing.T) {
	manager := versionreader.NewManager()
	require.NotNil(t, manager)

	testWithCancel(t, false, time.Second, func() {
		for i := 0; i < 10; i++ {
			manager.Wait(1)
		}
	})
}

func TestManager_Wait_WithReader_ShouldBlock(t *testing.T) {
	manager := versionreader.NewManager()
	require.NotNil(t, manager)

	testWithCancel(t, true, time.Millisecond*500, func() {
		for i := 0; i < 10; i++ {
			manager.Increment(1)
			manager.Wait(1)
		}
	})
}

func TestManager_Wait_WithReader_ShouldNotBlock(t *testing.T) {
	manager := versionreader.NewManager()
	require.NotNil(t, manager)

	decrementDelay := time.Millisecond * 500

	testWithCancel(t, false, decrementDelay*5, func() {
		manager.Increment(1)
		go func() {
			time.Sleep(decrementDelay)
			manager.Decrement(1)
		}()
		manager.Wait(1)
	})
}

func TestManager_Wait_DiffVersion_ShouldNotBlock(t *testing.T) {
	manager := versionreader.NewManager()
	require.NotNil(t, manager)

	testWithCancel(t, false, time.Second, func() {
		manager.Increment(1)
		manager.Wait(2)
	})
}

func TestManager_Wait_Timed(t *testing.T) {
	manager := versionreader.NewManager()
	require.NotNil(t, manager)

	for i := 0; i < 5; i++ {
		manager.Increment(1)

		decrementDuration := time.Second

		time.AfterFunc(decrementDuration, func() {
			manager.Increment(1)
			manager.Decrement(1)
			manager.Decrement(1)
		})

		timer := time.NewTimer(decrementDuration + time.Second)
		manager.Wait(1)

		require.True(t, timer.Stop())
	}
}

func TestManager_Parallel_One(t *testing.T) {
	numVersionsToTest := 1
	testParallel(t, numVersionsToTest)
}

func TestManager_Parallel_Two(t *testing.T) {
	numVersionsToTest := 2
	testParallel(t, numVersionsToTest)
}

func TestManager_Parallel_Five(t *testing.T) {
	numVersionsToTest := 5
	testParallel(t, numVersionsToTest)
}

func testParallel(t *testing.T, numVersions int) {
	manager := versionreader.NewManager()
	require.NotNil(t, manager)

	wg := sync.WaitGroup{}

	for v := int64(0); v < int64(numVersions); v++ {
		go func() {
			for i := 0; i < 10; i++ {
				go func() {
					wg.Add(1)
					manager.Increment(v)

					decrementDuration := time.Second

					time.AfterFunc(decrementDuration, func() {
						manager.Decrement(v)
					})

					timer := time.NewTimer(decrementDuration + time.Second)
					manager.Wait(v)

					require.True(t, timer.Stop())
					wg.Done()
				}()
			}
		}()
	}

	testWithCancel(t, false, 100*time.Second, func() {
		// All should eventually unblock
		for v := int64(0); v < int64(numVersions); v++ {
			for j := 0; j < 100; j++ {
				manager.Wait(v)
				time.Sleep(time.Millisecond * 20)
			}
		}
	})

	testWithCancel(t, false, 10*time.Second, func() {
		wg.Wait()
	})
}

func testWithCancel(t *testing.T, shouldTimeout bool, timeout time.Duration, fnUnderTest func()) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		fnUnderTest()
		cancel()
	}()

	select {
	case <-time.After(timeout):
		if !shouldTimeout {
			t.Error("timed out when should not have")
		}
	case <-ctx.Done():
		if shouldTimeout {
			t.Error("did not timeout when should have")
		}
		return
	}
}
