// Copyright 2016 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package remote

import (
	"errors"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/prometheus/client_golang/prometheus"

	"auto-monitor/common/log"
)

var (
	prometheusRedisElection = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "prometheus_redis_election",
		},
		[]string{"prometheus_id", "redis_replica"},
	)
)

func init() {
	prometheus.MustRegister(prometheusRedisElection)
}

type RedisClientPool struct {
	addr string
	Name string
	pool *redis.Pool
}

type Switcher struct {
	redisAddr     string
	prometheusId  string
	redisSwitcher bool
	redisReplica  string
	replicaWrite  bool
	redisPool     *RedisClientPool
	mtx           sync.RWMutex
}

func NewSwitcher(redisAddr, prometheusId, redisReplica string, redisSwitcher bool) (*Switcher, error) {
	switcher := &Switcher{
		redisAddr:     redisAddr,
		prometheusId:  prometheusId,
		redisSwitcher: redisSwitcher,
		redisReplica:  redisReplica,
	}
	err := switcher.judgeMaster()

	return switcher, err

}

func (s *Switcher) doUpdate(conn redis.Conn) {
	conn.Do("EXPIRE", s.prometheusId, 45)
	s.mtx.Lock()
	s.replicaWrite = true
	s.mtx.Unlock()
}

func (s *Switcher) judgeMaster() error {
	if s.redisAddr == "" || s.redisSwitcher == false || s.prometheusId == "" {
		s.replicaWrite = true
		return nil
	}
	redisPool, err := redisPoolInit(s.redisAddr)
	if err != nil {
		log.Errorf("redis.Dial %v \n", err.Error())
		return err
	}
	s.redisPool = redisPool
	first := true
	go func() {
		f := func() {
			if first {
				first = false
			} else {
				time.Sleep(time.Second * 30)
			}
			conn := s.redisPool.Get()
			if conn == nil {
				return
			}
			defer conn.Close()

			res, _ := conn.Do("SETNX", s.prometheusId, s.redisReplica)
			if err != nil {
				log.Errorf("SETNX %v \n", err.Error())
				return
			}
			if res == int64(1) {
				s.doUpdate(conn)
			} else {
				replica, err := redis.String(conn.Do("GET", s.prometheusId))
				if err != nil {
					log.Errorf("GET %v \n", err.Error())
					return
				}
				if replica == s.redisReplica {
					s.doUpdate(conn)
				} else {
					s.mtx.Lock()
					s.replicaWrite = false
					s.mtx.Unlock()
				}
			}

		}
		for {
			f()
			var val float64
			if s.replicaWrite {
				val = 1
			}
			prometheusRedisElection.WithLabelValues(s.prometheusId, s.redisReplica).Set(val)
		}
	}()

	return nil
}

func (s *Switcher) get() bool {
	return s.replicaWrite
}

//connInfo : 127.0.0.1:6379
func redisPoolInit(connInfo string) (*RedisClientPool, error) {
	redisPool := &RedisClientPool{
		Name: "redis-client-pool",
	}
	_ = redisPool.InitRedis(connInfo)

	return redisPool, redisPool.Get().Err()
}

func (p *RedisClientPool) InitRedis(addr string) error {
	dialFunc := func() (c redis.Conn, err error) {
		c, err = redis.Dial("tcp", addr)
		if err != nil {
			return nil, err
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
