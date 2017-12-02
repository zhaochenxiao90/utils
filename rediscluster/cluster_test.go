package rediscluster

import (
	"golangsrc/fmt"
	"testing"
)

func TestSetNx(t *testing.T) {
	ClusterInit("10.110.92.171:6379,10.110.92.171:6380,10.110.92.172:6379", "", 100)

	succ, err := SetNx("mynx", "mynxval", 0)
	if err != nil {
		t.Fatalf("setnx failed,err:%v", err.Error())
	}
	fmt.Printf("setnx res:%v\n", succ)
	val, err := Get("mynx")
	if err != nil {
		t.Fatalf("get failed,err:%s", err.Error())
	}
	t.Logf("val:%s", val)
}

func processKey(key string) error {
	fmt.Printf("process key:%s\n", key)
	return nil
}

func TestScan(t *testing.T) {
	ClusterInit("10.110.92.171:6379,10.110.92.171:6380,10.110.92.172:6379", "", 20000)

	Scan("B*", 100, processKey, 10)
}
