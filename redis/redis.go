package redis

import (
	"fmt"
	"strings"
	"sync"
	"time"

	redisgo "github.com/garyburd/redigo/redis"
)

var (
	redisInfo        RedisInfo
	defaultRedisPool *redisgo.Pool
	mux = &sync.Mutex{}
)

type RedisInfo struct {
	Addr      string
	PassWd    string
	Timeout   int
	MaxActive int
	MaxIdle   int
}

func RedisInit(addr, passwd string, timeout, active, idle int) {
	redisInfo.Addr = addr
	redisInfo.PassWd = passwd
	redisInfo.Timeout = timeout
	redisInfo.MaxActive = active
	redisInfo.MaxIdle = idle

	defaultRedisPool = pollInit(redisInfo)
}

func pollInit(info RedisInfo) *redisgo.Pool {
	return &redisgo.Pool{
		Dial: func() (redisgo.Conn, error) {
			var err error
			for _, addr := range strings.Split(info.Addr, ",") {
				c, err := redisgo.Dial("tcp4", addr,
					redisgo.DialConnectTimeout(time.Duration(info.Timeout)*time.Second),
				)
				if err != nil {
					continue
				}
				if info.PassWd != "" {
					_, err = c.Do("AUTH", info.PassWd)
					if err != nil {
						c.Close()
						continue
					}
					return c, nil
				}
				return c, nil
			}
			return nil, err
		},
		MaxIdle:     info.MaxIdle,
		MaxActive:   info.MaxActive,
		IdleTimeout: 60 * time.Second,
		TestOnBorrow: func(c redisgo.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

func SetString(key, val string) error {
	mux.Lock()
	c := defaultRedisPool.Get()
	mux.Unlock()
	defer c.Close()
	_, err := c.Do("SET", key, val)

	return err
}

func GetString(key string) (string, error) {
	mux.Lock()
	c := defaultRedisPool.Get()
	mux.Unlock()
	defer c.Close()
	reply, err := redisgo.String(c.Do("GET", key))

	if err != nil {
		return "", err
	}
	return reply, nil
}

func SetInt64(key string, val int64) error {
	mux.Lock()
	c := defaultRedisPool.Get()
	mux.Unlock()
	defer c.Close()
	_, err := c.Do("SET", key, val)

	return err
}

func GetInt64(key string) (int64, error) {
	mux.Lock()
	c := defaultRedisPool.Get()
	mux.Unlock()
	defer c.Close()
	reply, err := redisgo.Int64(c.Do("GET", key))

	if err != nil {
		return 0, err
	}
	return reply, nil
}

func HashMultiGet(fields ...interface{}) ([]string, error) {
	mux.Lock()
	c := defaultRedisPool.Get()
	mux.Unlock()
	defer c.Close()

	return redisgo.Strings(c.Do("HMGET", fields...))
}

func HashMultiSet(param ...interface{}) {
	mux.Lock()
	c := defaultRedisPool.Get()
	mux.Unlock()
	defer c.Close()

	c.Do("HMSET", param...)
}

func HashGetAll(hashKey string) ([]string, error) {
	mux.Lock()
	c := defaultRedisPool.Get()
	mux.Unlock()
	defer c.Close()

	ret, err := c.Do("HGETALL", hashKey)
	if err != nil {
		return nil, err
	}
	if ret != nil {
		ret, err := redisgo.Strings(ret, nil)
		return ret, err
	}
	return nil, nil
}

func HashGet(hashKey string, key string) ([]byte, error) {
	mux.Lock()
	c := defaultRedisPool.Get()
	mux.Unlock()
	defer c.Close()

	ret, err := c.Do("HGET", hashKey, key)
	if err != nil {
		return nil, err
	}
	if ret != nil {
		ret, err := redisgo.Bytes(ret, nil)
		return ret, err
	}
	return nil, nil
}

func HashSet(hashKey string, key string, val []byte) (int, error) {
	mux.Lock()
	c := defaultRedisPool.Get()
	mux.Unlock()
	defer c.Close()

	return redisgo.Int(c.Do("HSET", hashKey, key, val))
}

func HashDel(hashKey, field string) (int, error) {
	mux.Lock()
	c := defaultRedisPool.Get()
	mux.Unlock()
	defer c.Close()

	return redisgo.Int(c.Do("HDEL", hashKey, field))
}

func HashKeys(hashKey string) ([]string, error) {
	mux.Lock()
	c := defaultRedisPool.Get()
	mux.Unlock()
	defer c.Close()

	return redisgo.Strings(c.Do("HKEYS", hashKey))
}

func Incr(key string) (int64, error) {
	mux.Lock()
	c := defaultRedisPool.Get()
	mux.Unlock()
	defer c.Close()

	return redisgo.Int64(c.Do("INCR", key))
}

func IncrBy(key string, cnt int64) (int64, error) {
	mux.Lock()
	c := defaultRedisPool.Get()
	mux.Unlock()
	defer c.Close()

	return redisgo.Int64(c.Do("INCRBY", key, cnt))
}

func Expire(key string, t int64) (int64, error) {
	mux.Lock()
	c := defaultRedisPool.Get()
	mux.Unlock()
	defer c.Close()

	return redisgo.Int64(c.Do("EXPIRE", key, t))
}

func Ttl(key string) (int64, error) {
	mux.Lock()
	c := defaultRedisPool.Get()
	mux.Unlock()
	defer c.Close()

	return redisgo.Int64(c.Do("TTL", key))
}

func Delete(key string) (int64, error) {
	mux.Lock()
	c := defaultRedisPool.Get()
	mux.Unlock()
	defer c.Close()

	return redisgo.Int64(c.Do("DEL", key))
}

func Exist(key ...interface{})(int64, error) {
	mux.Lock()
	c := defaultRedisPool.Get()
	mux.Unlock()
	defer c.Close()

	return redisgo.Int64(c.Do("EXISTS", key...))
}

// 处理scan返回key的函数定义
type ScanProcFunc func(key string) error

func Scan(keyFlag string, count int, scanFunc ScanProcFunc, threadNum int) error {
	var scanPos int64 = 0
	scanLock := sync.Mutex{}
	exitFlag := false
	wg := &sync.WaitGroup{}

	// SCAN匹配关键字
	key := keyFlag
	if key == "" {
		key = "*"
	}
	// 每次返回结果条数
	tmpCount := 10
	if count > 0 {
		tmpCount = count
	}

	for i := 0; i < threadNum; i++ {
		index := i
		wg.Add(1)
		go func(index int) error {
			defer wg.Done()
			mux.Lock()
			c := defaultRedisPool.Get()
			mux.Unlock()
			defer c.Close()

			for {
				scanLock.Lock()
				if exitFlag {
					scanLock.Unlock()
					return nil
				}
				// 执行一次scan操作
				tmpRes, err := c.Do("SCAN", scanPos, "MATCH", key, "COUNT", tmpCount)
				if err != nil {
					scanLock.Unlock()
					return fmt.Errorf("scan thread:%d scan failed,err:%s", index, err.Error())
				}
				res := tmpRes.([]interface{})
				// 返回结果数组长度不为2,则格式错误
				if len(res) != 2 {
					scanLock.Unlock()
					return fmt.Errorf("scan thread:%d scan res len is:%d,not 2", index, len(res))
				}
				// 获取下一次scan位置
				var tmpNum int64 = 0
				fmt.Sscanf(string(res[0].([]byte)), "%d", &tmpNum)
				if tmpNum > 0 {
					scanPos = tmpNum
				} else {
					exitFlag = true
				}
				scanLock.Unlock()
				if scanFunc != nil {
					for _, v := range res[1].([]interface{}) {
						go func(key string) {
							scanFunc(key)
						}(string(v.([]byte)))
					}
				}
			}
			return nil
		}(index)
	}
	wg.Wait()
	return nil
}

// 处理scan返回key的函数定义
type HScanProcFunc func(key, field string) error

func HScan(hashKey, fieldFlag string, count int, scanFunc HScanProcFunc, threadNum int) error {
	var scanPos int64 = 0
	scanLock := sync.Mutex{}
	exitFlag := false
	wg := &sync.WaitGroup{}

	// SCAN匹配关键字
	key := fieldFlag
	if key == "" {
		key = "*"
	}
	// 每次返回结果条数
	tmpCount := 10
	if count > 0 {
		tmpCount = count
	}

	for i := 0; i < threadNum; i++ {
		in := i
		wg.Add(1)
		go func(index int) error {
			defer wg.Done()
			mux.Lock()
			c := defaultRedisPool.Get()
			mux.Unlock()
			defer c.Close()

			for {
				scanLock.Lock()
				if exitFlag {
					scanLock.Unlock()
					return nil
				}
				// 执行一次scan操作
				tmpRes, err := c.Do("HSCAN", hashKey, scanPos, "MATCH", key, "COUNT", tmpCount)
				if err != nil {
					scanLock.Unlock()
					return fmt.Errorf("hscan thread:%d scan failed,err:%s", index, err.Error())
				}
				res := tmpRes.([]interface{})
				// 返回结果数组长度不为2,则格式错误
				if len(res) != 2 {
					scanLock.Unlock()
					return fmt.Errorf("hscan thread:%d scan res len is:%d,not 2", index, len(res))
				}
				// 获取下一次scan位置
				var tmpNum int64 = 0
				fmt.Sscanf(string(res[0].([]byte)), "%d", &tmpNum)
				if tmpNum > 0 {
					scanPos = tmpNum
				} else {
					exitFlag = true
				}
				scanLock.Unlock()
				if scanFunc != nil {
					v2 := res[1].([]interface{})
					resLen := len(v2)
					if resLen > 0 {
						for i := 0; i < resLen; i += 2 {
							go func(key, field string) {
								scanFunc(key, field)
							}(hashKey, string(v2[i].([]byte)))
						}
					}
				}
			}
			return nil
		}(in)
	}
	wg.Wait()
	return nil
}
