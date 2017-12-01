package redis

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestSetString(t *testing.T) {
	RedisInit("10.110.92.171:6379,10.110.92.171:6380,10.110.92.172:6379", "", 3, 3, 1)

	for i := 0; i < 100; i++ {
		go SetString("aaa", "bbb")
	}

	err := SetString("testkey", "testvalue")
	if err != nil {
		//t.Fatalf("set testkey failed,err:%s", err.Error())
	}
	s, err := GetString("testkey")
	if err != nil {
		//t.Fatalf("get testkey failed,err:%s", err.Error())
	}
	t.Logf("testkey:%s", s)
	Delete("testkey")
}

func BenchmarkSetString(b *testing.B) {
	RedisInit("10.135.29.168:16379", "wangchunyan1@le.com", 3, 10000, 10)
	wg := sync.WaitGroup{}
	b.N = 10000
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func(num int) {
			defer wg.Done()
			err := SetString(fmt.Sprintf("key_%d", num), fmt.Sprintf("value_%d", num))
			if err != nil {
				b.Fatalf("set failed,err:%s", err.Error())
			}
		}(i)
	}
	wg.Wait()
	b.Logf("set string ok")
}

func TestSetInt64(t *testing.T) {
	RedisInit("10.135.29.168:16379", "wangchunyan1@le.com", 3, 3, 1)

	err := SetInt64("testint64", 88888888)
	if err != nil {
		t.Fatalf("setint64 failed,err:%s", err.Error())
	}
	v, err := GetInt64("testint64")
	if err != nil {
		t.Fatalf("testint64 value:%d", v)
	}
	t.Logf("testint64 value:%d", v)
	Delete("testint64")
}

func TestHashKeys(t *testing.T) {
	RedisInit("10.135.29.168:16379", "wangchunyan1@le.com", 3, 3, 1)

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
	Delete("testhashkeys")
}

func TestIncr(t *testing.T) {
	RedisInit("10.135.29.168:16379", "wangchunyan1@le.com", 3, 3, 1)

	Delete("incrkey")
	n, err := Incr("incrkey")
	if err != nil {
		t.Fatalf("set hashkey failed,err:%s", err.Error())
	}
	t.Logf("set hash key n is:%d", n)
	Incr("incrkey")
	n, err = GetInt64("incrkey")
	if err != nil || n != 2 {
		t.Fatalf("incr failed")
	}
	Delete("incrkey")
}

func TestIncrBy(t *testing.T) {
	RedisInit("10.135.29.168:16379", "wangchunyan1@le.com", 3, 3, 1)

	Delete("incrbykey")
	n, err := IncrBy("incrbykey", 5)
	if err != nil {
		t.Fatalf("set TestIncrBy failed,err:%s", err.Error())
	}
	t.Logf("set hash key n is:%d", n)
	IncrBy("incrbykey", 5)
	n, err = GetInt64("incrbykey")
	if err != nil || n != 10 {
		t.Fatalf("incr failed")
	}
	t.Logf("set hash key n is:%d", n)
	Delete("incrbykey")
}

func TestExpire(t *testing.T) {
	RedisInit("10.135.29.168:16379", "wangchunyan1@le.com", 3, 3, 1)

	Delete("expirerkey")
	SetString("expirerkey", "expirerkey")
	Expire("expirerkey", 60)
}

func TestTtl(t *testing.T) {
	RedisInit("10.135.29.168:16379", "wangchunyan1@le.com", 3, 3, 1)

	Delete("expirerkey")
	SetString("expirerkey", "expirerkey")
	Expire("expirerkey", 60)
	time.Sleep(5 * time.Second)

	ttl, err := Ttl("expirerkey")
	if err != nil {
		t.Fatalf("ttl failed,err:%s", err.Error())
	}
	t.Logf("ttl:%d", ttl)
	Delete("expirerkey")
}

func echoKey(key string) error {
	//fmt.Printf("process key:%s\n", key)
	return nil
}

func TestScan(t *testing.T) {
	RedisInit("10.135.29.168:16379", "wangchunyan1@le.com", 3, 10000, 10)
	for i := 0; i < 100; i++ {
		SetString(fmt.Sprintf("key_%d", i), fmt.Sprintf("value_%d", i))
	}
	Scan("", 10, echoKey, 3)
	time.Sleep(3 * time.Second)
	for i := 0; i < 100; i++ {
		Delete(fmt.Sprintf("key_%d", i))
	}
}

func echoHashKey(key, field string) error {
	//fmt.Printf("key:%s,field:%s\n", key, field)
	return nil
}

func TestHScan(t *testing.T) {
	RedisInit("10.135.29.168:16379", "wangchunyan1@le.com", 3, 10000, 10)
	for i := 0; i < 100; i++ {
		HashSet("testhashscan", fmt.Sprintf("key_%d", i), []byte(fmt.Sprintf("value_%d", i)))
	}
	HScan("testhashscan", "", 10, echoHashKey, 3)
	time.Sleep(1 * time.Second)
	Delete("testhashscan")
}

func TestExist(t *testing.T) {
	RedisInit("10.135.29.168:16379", "wangchunyan1@le.com", 3, 10000, 10)

	SetString("exkey", "exvalue")
	n, err := Exist("exkey")
	if err != nil {
		t.Fatalf("exist failed,err:%s", err.Error())
	}
	if n == 1 {
		t.Logf("exist exkey")
	}
}
