package atomic

import "testing"

func TestIncrDefUInt64(t *testing.T) {
	t.Logf("def cnt:%d", GetDefUInt64())
	IncrDefUInt64()
	t.Logf("def cnt:%d,should be 1", GetDefUInt64())
	IncrDefUInt64()
	t.Logf("def cnt:%d,should be 2", GetDefUInt64())

	DecrDefUInt64()
	t.Logf("def cnt:%d,should be 1", GetDefUInt64())
	ResetDefUInt64()
	t.Logf("def cnt:%d,should be 0", GetDefUInt64())

	var customNum uint64 = 100
	t.Logf("def cnt:%d,should be 100", GetUInt64(&customNum))
	IncrUInt64(&customNum)
	t.Logf("def cnt:%d,should be 101", GetUInt64(&customNum))
	IncrUInt64(&customNum)
	t.Logf("def cnt:%d,should be 102", GetUInt64(&customNum))

	DecrUInt64(&customNum)
	t.Logf("def cnt:%d,should be 101", GetUInt64(&customNum))
	ResetUInt64(&customNum)
	t.Logf("def cnt:%d,should be 0", GetUInt64(&customNum))
}
