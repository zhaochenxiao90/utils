package redis

import (
	"strings"
	"time"

	redisgo "github.com/garyburd/redigo/redis"
)

var (
	defaultRedisPool *redisgo.Pool
	redisInfo        RedisInfo
)

type RedisInfo struct {
	Addr    string
	PassWd  string
	Timeout int
}

func RedisInit(addr, passwd string, timeout int) {
	redisInfo.Addr = addr
	redisInfo.PassWd = passwd
	redisInfo.Timeout = timeout

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
		MaxIdle:     3,
		MaxActive:   3,
		IdleTimeout: 60 * time.Second,
		TestOnBorrow: func(c redisgo.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

func SetString(key, val string) error {
	c := defaultRedisPool.Get()
	defer c.Close()
	_, err := c.Do("SET", key, val)

	return err
}

func GetString(key string) (string, error) {
	c := defaultRedisPool.Get()
	defer c.Close()
	reply, err := redisgo.String(c.Do("GET", key))

	if err != nil {
		return "", err
	}
	return reply, nil
}

func SetInt64(key string, val int64) error {
	c := defaultRedisPool.Get()
	defer c.Close()
	_, err := c.Do("SET", key, val)

	return err
}

func GetInt64(key string) (int64, error) {
	c := defaultRedisPool.Get()
	defer c.Close()
	reply, err := redisgo.Int64(c.Do("GET", key))

	if err != nil {
		return 0, err
	}
	return reply, nil
}

func HashMultiGet(fields ...interface{}) ([]string, error) {
	c := defaultRedisPool.Get()
	defer c.Close()

	return redisgo.Strings(c.Do("HMGET", fields...))
}

func HashMultiSet(param ...interface{}) {
	c := defaultRedisPool.Get()
	defer c.Close()

	c.Do("HMSET", param...)
}

func HashGetAll(hashKey string) ([]string, error) {
	c := defaultRedisPool.Get()
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
	c := defaultRedisPool.Get()
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
	c := defaultRedisPool.Get()
	defer c.Close()

	return redisgo.Int(c.Do("HSET", hashKey, key, val))
}

func HashKeys(hashKey string) ([]string, error) {
	c := defaultRedisPool.Get()
	defer c.Close()

	return redisgo.Strings(c.Do("HKEYS", hashKey))
}
