package rediscluster

import (
	"fmt"
	"testing"
	"time"
)

func TestSetNx(t *testing.T) {
	ClusterInit("10.110.92.171:6379,10.110.92.171:6380,10.110.92.172:6379", "", 100)

	succ, err := SetNx("mynx", "mynxval", 0)
	if err != nil {
		t.Fatalf("setnx failed,err:%v", err.Error())
	}
	fmt.Printf("setnx res:%v\n", succ)
	val, err := GetStr("mynx")
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

	Scan("AAA*", 100, processKey, 10)
}

func TestExpire(t *testing.T) {
	ClusterInit("10.110.92.171:6379,10.110.92.171:6380,10.110.92.172:6379", "", 20000)

	SetNx("exkey", "exvalue", 0)
	Expire("exkey", 10)
	time.Sleep(3 * time.Second)
	ttl, err := TTL("exkey")
	if err != nil {
		t.Fatalf("ttl failed,err:%s", err.Error())
	}
	t.Logf("ttl is:%d", ttl)
	Del("exkey")
}

func TestGetInt64(t *testing.T) {
	ClusterInit("10.110.92.171:6379,10.110.92.171:6380,10.110.92.172:6379", "", 20000)

	SetNx("exkeyint", 10, 0)
	v, err := GetInt64("exkeyint")
	if err != nil {
		t.Fatalf("GetInt64 failed,err:%s", err.Error())
	}
	t.Logf("v is:%d", v)

	v, err = GetInt64("exkeyint222")
	if err != nil {
		t.Fatalf("GetInt64 failed,err:%s", err.Error())
	}
}

func TestIncrBy(t *testing.T) {
	ClusterInit("10.110.92.171:6379,10.110.92.171:6380,10.110.92.172:6379", "", 20000)

	SetNx("exkeyint", 10, 0)
	IncrBy("exkeyint", 5)
	v, err := GetInt64("exkeyint")
	if err != nil {
		t.Fatalf("GetInt64 failed,err:%s", err.Error())
	}
	t.Logf("v is:%d", v)

}
