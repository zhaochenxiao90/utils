package rediscluster

import (
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis"
)

type RedisCluster struct {
	client *redis.ClusterClient
}

var (
	DefaultCluster *RedisCluster
)

func ClusterInit(addrs, pwd string, poolsize int) {
	DefaultCluster = &RedisCluster{
		client: newClusterClient(addrs, pwd, poolsize),
	}
}

func NewRedisCluster(addrs, pwd string, poolsize int) *RedisCluster {
	cluster := &RedisCluster{
		client: newClusterClient(addrs, pwd, poolsize),
	}
	return cluster
}

func newClusterClient(addrs, pwd string, poolsize int) *redis.ClusterClient {
	ops := &redis.ClusterOptions{
		Addrs:      strings.Split(addrs, ","),
		ReadOnly:   true,
		MaxRetries: 3,
		Password:   pwd,

		DialTimeout:  time.Duration(3) * time.Second,
		ReadTimeout:  time.Duration(60) * time.Second,
		WriteTimeout: time.Duration(3) * time.Second,

		PoolSize:    poolsize,
		PoolTimeout: time.Duration(30) * time.Second,
		IdleTimeout: time.Duration(30) * time.Second,
	}
	return redis.NewClusterClient(ops)
}

func boolCmdRes(cmd *redis.BoolCmd) (bool, error) {
	return cmd.Val(), cmd.Err()
}

func stringCmdRes(cmd *redis.StringCmd) (string, error) {
	return cmd.Val(), cmd.Err()
}

func stringCmdInt64Res(cmd *redis.StringCmd) (int64, error) {
	return cmd.Int64()
}

func intCmdRes(cmd *redis.IntCmd) (int64, error) {
	return cmd.Val(), cmd.Err()
}

func duraCmdRes(cmd *redis.DurationCmd) (int64, error) {
	return int64(cmd.Val()) / int64(time.Second), cmd.Err()
}

func (c *RedisCluster) SetNx(key string, val interface{}, expire int64) (bool, error) {
	return boolCmdRes(c.client.SetNX(key, val, time.Duration(expire)*time.Second))
}

func SetNx(key string, val interface{}, expire int64) (bool, error) {
	return boolCmdRes(DefaultCluster.client.SetNX(key, val, time.Duration(expire)*time.Second))
}

func (c *RedisCluster) Expire(key string, expire int64) (bool, error) {
	return boolCmdRes(c.client.Expire(key, time.Duration(expire)*time.Second))
}

func Expire(key string, expire int64) (bool, error) {
	return boolCmdRes(DefaultCluster.client.Expire(key, time.Duration(expire)*time.Second))
}

func GetStr(key string) (string, error) {
	return stringCmdRes(DefaultCluster.client.Get(key))
}

func (c *RedisCluster) GetStr(key string) (string, error) {
	return stringCmdRes(c.client.Get(key))
}

func GetInt64(key string) (int64, error) {
	return stringCmdInt64Res(DefaultCluster.client.Get(key))
}

func (c *RedisCluster) GetInt64(key string) (int64, error) {
	return stringCmdInt64Res(c.client.Get(key))
}

func Del(key ...string) (int64, error) {
	return intCmdRes(DefaultCluster.client.Del(key...))
}

func (c *RedisCluster) Del(key ...string) (int64, error) {
	return intCmdRes(c.client.Del(key...))
}

func TTL(key string) (int64, error) {
	return duraCmdRes(DefaultCluster.client.TTL(key))
}

func (c *RedisCluster) TTL(key string) (int64, error) {
	return duraCmdRes(c.client.TTL(key))
}

func IncrBy(key string, value int64) (int64, error) {
	return intCmdRes(DefaultCluster.client.IncrBy(key, value))
}

func (c *RedisCluster) IncrBy(key string, value int64) (int64, error) {
	return intCmdRes(c.client.IncrBy(key, value))
}

// 处理scan返回key的函数定义
type ScanProcFunc func(key string) error

func Scan(match string, count int64, scanFunc ScanProcFunc, threadNum int) error {
	var scanPos uint64 = 0
	scanLock := sync.Mutex{}
	exitFlag := false

	// SCAN匹配关键字
	matchKey := match
	if matchKey == "" {
		matchKey = "*"
	}
	// 每次返回条数
	var scanCnt int64 = 100
	if count > 0 {
		scanCnt = count
	}

	wg := &sync.WaitGroup{}
	for i := 0; i < threadNum; i++ {
		wg.Add(1)
		go func(index int) error {
			defer wg.Done()
			for {
				scanLock.Lock()
				if exitFlag {
					scanLock.Unlock()
					return nil
				}
				cmd := DefaultCluster.client.Scan(scanPos, matchKey, scanCnt)
				if cmd.Err() != nil {
					scanLock.Unlock()
					return cmd.Err()
				}
				keys, pos := cmd.Val()
				if pos > 0 {
					scanPos = pos
				} else {
					exitFlag = true
				}
				scanLock.Unlock()
				if scanFunc != nil {
					for _, k := range keys {
						scanFunc(k)
					}
				}
			}
			return nil
		}(i)
	}
	wg.Wait()
	return nil
}
