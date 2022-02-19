package versionreader

import "sync"

type Waiter interface{
	Wait()
	Add()
	Done() bool
}

type Reader struct {
	counter int64
	zeroWaiterLock *sync.Mutex
	wg 	*sync.WaitGroup
	wgWaiter *sync.WaitGroup
}

func NewReader() *Reader {
	return &Reader{
		counter: 0,
		zeroWaiterLock: &sync.Mutex{},
		wg: &sync.WaitGroup{},
		wgWaiter: &sync.WaitGroup{},
	}
}

func (vr *Reader) Wait() {
	vr.wgWaiter.Add(1)
	vr.wg.Wait()
	vr.wgWaiter.Done()
}

func (vr *Reader) Add() {
	vr.zeroWaiterLock.Lock()
	defer vr.zeroWaiterLock.Unlock()

	// You can't add a reader if wg.Wait() was called with no readers
	// Doing so would cause a panic.
	if vr.counter == 0 {
		vr.wgWaiter.Wait()
	}

	vr.wg.Add(1)
	vr.counter++
}

// Done should be called when a version reader is Done reading.
// returns true if this was the last reader
func (vr *Reader) Done() bool {
	vr.zeroWaiterLock.Lock()
	defer vr.zeroWaiterLock.Unlock()
	if vr.counter > 0 {
		vr.wg.Done()
		vr.counter--
		return vr.counter == 0
	}
	return false
}