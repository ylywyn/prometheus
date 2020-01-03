package rpc

import (
	"runtime"
	"sync"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"

	"auto-monitor/common/log"
)

//6 hour
const cacheTimeout = 6 * 3600

type cacheEntry struct {
	t    int64
	ref  uint64
	lset labels.Labels
}

type SeriesCache struct {
	worker     *Worker
	seriesLock sync.RWMutex
	series     map[string]*cacheEntry

	fastSeriesLock sync.RWMutex
	fastSeries     map[uint64]*cacheEntry
}

func NewSeriesCache(w *Worker) *SeriesCache {
	c := &SeriesCache{
		worker:     w,
		series:     make(map[string]*cacheEntry, 1024000),
		fastSeries: make(map[uint64]*cacheEntry, 1024000),
	}
	return c
}

func (c *SeriesCache) get(key string, t int64) (*cacheEntry, bool) {
	c.seriesLock.RLock()
	e, ok := c.series[key]
	c.seriesLock.RUnlock()
	if !ok {
		return nil, false
	}
	e.t = t
	return e, true
}

func (c *SeriesCache) getFast(key uint64, t int64) (*cacheEntry, bool) {
	c.fastSeriesLock.RLock()
	e, ok := c.fastSeries[key]
	c.fastSeriesLock.RUnlock()
	if !ok {
		return nil, false
	}
	e.t = t
	return e, true
}

func (c *SeriesCache) add(key string, ref uint64, lset labels.Labels, t int64) {
	if ref == 0 {
		return
	}
	c.seriesLock.Lock()
	c.series[key] = &cacheEntry{ref: ref, lset: lset, t: t}
	c.seriesLock.Unlock()
}

func (c *SeriesCache) addFast(key, ref uint64, lset labels.Labels, t int64) {
	if ref == 0 {
		return
	}
	c.fastSeriesLock.Lock()
	c.fastSeries[key] = &cacheEntry{ref: ref, lset: lset, t: t}
	c.fastSeriesLock.Unlock()
}

func (c *SeriesCache) clearTimeout() {
	log.Infof("worker %d begin clean cahce.", c.worker.index)

	t := time.Now().Unix()

	// string keys
	keys := make([]string, 0, len(c.series)/20)
	c.seriesLock.RLock()
	for k, v := range c.series {
		if t-v.t > cacheTimeout {
			keys = append(keys, k)
		}
	}
	c.seriesLock.RUnlock()

	count := len(keys)
	if count > 0 {
		i := 0
		for _, k := range keys {
			c.seriesLock.Lock()
			delete(c.series, k)
			c.seriesLock.Unlock()
			i += 1
			if i%2000 == 0 {
				time.Sleep(time.Millisecond * 20)
			}
		}
	}

	//int keys
	intKeys := make([]uint64, 0, len(c.fastSeries)/20)
	c.fastSeriesLock.RLock()
	for k, v := range c.fastSeries {
		if t-v.t > cacheTimeout {
			intKeys = append(intKeys, k)
		}
	}
	c.fastSeriesLock.RUnlock()

	intCount := len(intKeys)
	if intCount > 0 {
		i := 0
		for _, k := range intKeys {
			c.fastSeriesLock.Lock()
			delete(c.fastSeries, k)
			c.fastSeriesLock.Unlock()

			i += 1
			if i%2000 == 0 {
				time.Sleep(time.Millisecond * 20)
			}
		}
	}

	log.Infof("worker %d  has clean cahce: %d ok.", c.worker.index, count+intCount)
	if c.worker.index == 0 {
		t := time.NewTimer(time.Minute * 2)
		defer t.Stop()

		<-t.C
		runtime.GC()
	}
}
