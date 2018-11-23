package server

import (
	"sync/atomic"
)

type aint struct{ v uintptr }

func (a *aint) add(d int) int {
	if d < 0 {
		return int(atomic.AddUintptr(&a.v, ^uintptr((d*-1)-1)))
	}
	return int(atomic.AddUintptr(&a.v, uintptr(d)))
}
func (a *aint) get() int {
	return int(atomic.LoadUintptr(&a.v))
}
func (a *aint) set(i int) int {
	return int(atomic.SwapUintptr(&a.v, uintptr(i)))
}

type abool struct{ v uint32 }

func (a *abool) on() bool {
	return atomic.LoadUint32(&a.v) != 0
}
func (a *abool) set(t bool) bool {
	if t {
		return atomic.SwapUint32(&a.v, 1) != 0
	}
	return atomic.SwapUint32(&a.v, 0) != 0
}
