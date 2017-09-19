package redis

import (
	"testing"
)

func TestSetString(t *testing.T) {
	RedisInit("10.112.38.64:16379", "", 3)

	err := SetString("testkey", "testvalue")
	if err != nil {
		t.Fatalf("set testkey failed,err:%s", err.Error())
	}
	s, err := GetString("testkey")
	if err != nil {
		t.Fatalf("get testkey failed,err:%s", err.Error())
	}
	t.Logf("testkey:%s", s)
}

func TestSetInt64(t *testing.T)  {
	RedisInit("10.112.38.64:16379", "", 3)

	err := SetInt64("testint64", 88888888)
	if err != nil {
		t.Fatalf("setint64 failed,err:%s", err.Error())
	}
	v, err := GetInt64("testint64")
	if err != nil {
		t.Fatalf("testint64 value:%d", v)
	}
	t.Logf("testint64 value:%d", v)
}

func TestHashKeys(t *testing.T) {
	RedisInit("10.112.38.64:16379", "", 3)

	n, err := HashSet("testhashkeys", "onekey", []byte("onvalue"))
	if err != nil {
		t.Fatalf("set hashkey failed,err:%s", err.Error())
	}
	t.Logf("set hash key n is:%d", n)

	keys, err := HashKeys("testhashkeys")
	if err != nil {
		t.Fatalf("set HashKeys failed,err:%s", err.Error())
	}
	t.Logf("keys:%v", keys)
}