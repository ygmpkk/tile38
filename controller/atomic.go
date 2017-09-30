package controller

import "sync/atomic"

type aint struct{ v int64 }

func (a *aint) add(d int) int {
	return int(atomic.AddInt64(&a.v, int64(d)))
}
func (a *aint) get() int {
	return int(atomic.LoadInt64(&a.v))
}
func (a *aint) set(i int) int {
	return int(atomic.SwapInt64(&a.v, int64(i)))
}

type abool struct{ v int64 }

func (a *abool) on() bool {
	return atomic.LoadInt64(&a.v) != 0
}
func (a *abool) set(t bool) bool {
	if t {
		return atomic.SwapInt64(&a.v, 1) != 0
	}
	return atomic.SwapInt64(&a.v, 0) != 0
}
