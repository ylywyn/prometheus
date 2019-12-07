package rpc

import (
	"sync"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
)

//6 hour
const cacheTimeout = 6 * 3600

type cacheEntry struct {
	t    int64
	ref  uint64
	lset labels.Labels
}

type SeriesCache struct {
	sync.RWMutex
	series map[string]*cacheEntry
}

func NewSeriesCache() *SeriesCache {
	c := &SeriesCache{
		series: make(map[string]*cacheEntry, 2048000),
	}
	return c
}

func (c *SeriesCache) get(met string, t int64) (*cacheEntry, bool) {
	c.RLock()
	e, ok := c.series[met]
	c.RUnlock()
	if !ok {
		return nil, false
	}
	e.t = t
	return e, true
}

func (c *SeriesCache) add(met string, ref uint64, lset labels.Labels) {
	if ref == 0 {
		return
	}
	c.Lock()
	c.series[met] = &cacheEntry{ref: ref, lset: lset}
	c.Unlock()
}

func (c *SeriesCache) clearTimeout() {
	t := time.Now().Unix()

	keys := make([]string, 0, len(c.series)/20)

	c.RLock()
	for k, v := range c.series {
		if t-v.t > cacheTimeout {
			keys = append(keys, k)
		}
	}
	c.RUnlock()

	if len(keys) > 0 {
		for _, k := range keys {
			c.Lock()
			delete(c.series, k)
			c.Unlock()
		}
	}
}
