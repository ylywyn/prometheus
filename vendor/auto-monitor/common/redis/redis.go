package redis

import (
	"errors"
	"time"

	"github.com/garyburd/redigo/redis"

	"auto-monitor/insight/utils/log"
)

var RedisPool *RedisClientPool

//connInfo : 127.0.0.1:6379
func InitPool(connInfo string) error {
	log.Debugf("connect redis info :%s", connInfo)

	RedisPool = &RedisClientPool{
		Name: "redis-client-pool",
	}
	err := RedisPool.InitRedis(connInfo)
	if err != nil {
		log.Errorf("redis init failed:", err.Error())
	} else {
		log.Info("connect redis ok!")
	}
	return err
}

type RedisClientPool struct {
	addr string
	Name string
	pool *redis.Pool
}

func (p *RedisClientPool) InitRedis(addr string) error {
	dialFunc := func() (c redis.Conn, err error) {
		c, err = redis.Dial("tcp", addr)
		if err != nil {
			return
		}
		_, err = c.Do("SELECT", 0)
		if err != nil {
			c.Close()
			return nil, err
		}
		return c, nil
	}
	p.addr = addr

	// initialize a new pool
	p.pool = &redis.Pool{
		MaxIdle:     6,
		IdleTimeout: 240 * time.Second,
		Dial:        dialFunc,
	}

	//test
	if p.Get() != nil {
		return nil
	}
	return errors.New("Redis connect failed")
}

func (p *RedisClientPool) Get() redis.Conn {
	if p.pool != nil {
		return p.pool.Get()
	}
	return nil
}

func (p *RedisClientPool) Close() {
	p.pool.Close()
}
