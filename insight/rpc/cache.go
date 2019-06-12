package rpc

import (
	"github.com/prometheus/prometheus/pkg/labels"
)

type cacheEntry struct {
	ref  uint64
	hash uint64
	lset labels.Labels
}

type SeriesCache struct {
	series map[string]*cacheEntry
}

func NewSeriesCache() *SeriesCache {
	c := &SeriesCache{}
	c.clear()
	return c
}

func (c *SeriesCache) get(met string) (*cacheEntry, bool) {
	e, ok := c.series[met]
	if !ok {
		return nil, false
	}

	return e, true
}

func (c *SeriesCache) add(met string, ref uint64, lset labels.Labels, hash uint64) {
	if ref == 0 {
		return
	}
	c.series[met] = &cacheEntry{ref: ref, lset: lset, hash: hash}
}

func (c *SeriesCache) clear() {
	c.series = make(map[string]*cacheEntry, 102400)
}
