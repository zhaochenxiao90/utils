package atomic

import (
	"sync/atomic"
)

var (
	default_atomic_uint64 uint64 = 0
)

func ResetDefUInt64() {
	atomic.StoreUint64(&default_atomic_uint64, 0)
}

func GetDefUInt64() uint64 {
	return atomic.LoadUint64(&default_atomic_uint64)
}

func IncrDefUInt64() {
	atomic.AddUint64(&default_atomic_uint64, 1)
}

func DecrDefUInt64() {
	atomic.AddUint64(&default_atomic_uint64, ^uint64(0))
}

func ResetUInt64(num *uint64) {
	atomic.StoreUint64(num, 0)
}

func GetUInt64(num *uint64) uint64 {
	return atomic.LoadUint64(num)
}

func IncrUInt64(num *uint64) {
	atomic.AddUint64(num, 1)
}

func DecrUInt64(num *uint64) {
	atomic.AddUint64(num, ^uint64(0))
}