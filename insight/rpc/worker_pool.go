package rpc

import (
	"hash"
	"sync"

	"github.com/prometheus/prometheus/storage"
	"github.com/reusee/mmh3"

	"auto-insight/common/log"
	"auto-insight/common/rpc/gen-go/metrics"
	"auto-insight/common/strconv"
)

var hashPool32 = sync.Pool{
	New: func() interface{} {
		return mmh3.New32()
	},
}

var metricsPool = sync.Pool{
	New: func() interface{} {
		return make([]*metrics.Metric, 0, 32)
	},
}

type WorkerPool struct {
	sync.Mutex
	stopped  bool
	parallel int
	workers  []*Worker
	appender storage.Appender
}

func NewWorkerPool(parallel int, appender storage.Appender) *WorkerPool {
	p := &WorkerPool{
		stopped:  true,
		parallel: parallel,
		workers:  make([]*Worker, parallel),
		appender: appender,
	}

	for i := 0; i < parallel; i++ {
		p.workers[i] = NewWorker(i, appender)
	}

	return p
}

func (wp *WorkerPool) Run() {
	wp.Lock()
	defer wp.Unlock()

	for i := 0; i < wp.parallel; i++ {
		wp.workers[i].Run()
	}
}

func (wp *WorkerPool) Stop() {
	wp.Lock()
	defer wp.Unlock()

	if !wp.stopped {
		for i := 0; i < wp.parallel; i++ {
			wp.workers[i].Stop()
		}
	}
}

//将hash索引相同的 metric打到相同的sender, 使每个worker缓存一部分merics
func (wp *WorkerPool) Write(ms *metrics.Metrics) error {
	h := hashPool32.Get().(hash.Hash32)
	defer hashPool32.Put(h)

	msArray := make([][]*metrics.Metric, wp.parallel)
	for _, m := range ms.List {
		i := wp.workerIndex(h, m.MetricKey)
		if msArray[i] == nil {
			msArray[i] = metricsPool.Get().([]*metrics.Metric)
		}
		msArray[i] = append(msArray[i], m)
		if len(msArray[i]) >= 32 {
			if err := wp.workers[i].Storage(msArray[i]); err != nil {
				log.Errorf("worker storage error:%s", err.Error())
			}
			msArray[i] = nil
		}
	}
	for i, _ := range msArray {
		if msArray[i] != nil {
			if err := wp.workers[i].Storage(msArray[i]); err != nil {
				log.Errorf("worker storage error:%s", err.Error())
			}
		}
	}

	return nil
}

//
func (wp *WorkerPool) workerIndex(h hash.Hash32, series string) int {
	key := strconv.StrToBytes(series)
	if len(key) > 16 {
		key = key[:16]
	}

	h.Write(key)
	ret := h.Sum32()
	h.Reset()
	return int(ret) % wp.parallel
}
