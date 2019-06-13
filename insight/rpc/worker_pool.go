package rpc

import (
	"hash"
	"sync"
	"sync/atomic"
	"time"

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

var statusMetrics = []*metrics.Metric{
	&metrics.Metric{MetricKey: `prometheus_rpc_metrics{type="commited"}0`},
	&metrics.Metric{MetricKey: `prometheus_rpc_metrics{type="parsed"}0`},
	&metrics.Metric{MetricKey: `prometheus_rpc_metrics{type="received"}0`},
}

type Status struct {
	MetricReceived uint64
	MetricAdd      uint64
	MetricParsed   uint64
}

type WorkerPool struct {
	sync.Mutex
	stopped  bool
	parallel int
	workers  []*Worker

	status     Status
	commitChan chan int
	appender   storage.Appender
}

func NewWorkerPool(parallel int, appender storage.Appender) *WorkerPool {
	p := &WorkerPool{
		stopped:    true,
		parallel:   parallel,
		workers:    make([]*Worker, parallel),
		appender:   appender,
		commitChan: make(chan int, 512),
	}

	for i := 0; i < parallel; i++ {
		p.workers[i] = NewWorker(i, appender)
		p.workers[i].pool = p
	}

	return p
}

func (wp *WorkerPool) Run() {
	log.Info("WorkerPool Run...")

	wp.Lock()
	defer wp.Unlock()

	wp.stopped = false
	go wp.commitLoop()

	for i := 0; i < wp.parallel; i++ {
		wp.workers[i].Run()
	}
}

func (wp *WorkerPool) Stop() {
	log.Info("WorkerPool Stop")

	wp.Lock()
	defer wp.Unlock()

	if !wp.stopped {
		wp.stopped = true
		for i := 0; i < wp.parallel; i++ {
			wp.workers[i].Stop()
		}
		close(wp.commitChan)
	}
}

//将hash索引相同的 metric打到相同的sender, 使每个worker缓存一部分merics
func (wp *WorkerPool) Write(ms *metrics.Metrics) error {
	atomic.AddUint64(&wp.status.MetricReceived, uint64(len(ms.List)))

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
		if msArray[i] != nil && len(msArray[i]) > 0 {
			if err := wp.workers[i].Storage(msArray[i]); err != nil {
				log.Errorf("worker storage error:%s", err.Error())
			}
		}
	}

	return nil
}

//使用前16个字节，计算index
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

//统一批量Commit
func (wp *WorkerPool) AppenderCommit(added int, seriesAdded int) {
	select {
	case wp.commitChan <- added:
	default:
		log.Warnf("AppenderCommit timeout")
	}

	atomic.AddUint64(&wp.status.MetricAdd, uint64(added))
	atomic.AddUint64(&wp.status.MetricParsed, uint64(seriesAdded))
}

//
func (wp *WorkerPool) commitLoop() {
	loopCount := 0
	t := time.NewTicker(3 * time.Second)
	defer t.Stop()

	addCount := 0
	commit := func() {
		addCount = 0
		if err := wp.appender.Commit(); err != nil {
			log.Errorf("wp.appender.Commit error: %s", err.Error())
		}
	}

	for {
		select {
		case add, ok := <-wp.commitChan:
			if !ok {
				log.Debug("commitLoop quit")
				return
			}

			addCount += add
			if addCount >= 1024 {
				commit()
			}

		case <-t.C:
			//打印统计
			loopCount ++
			if loopCount >= 20 {
				loopCount = 0
				wp.reportStatus()
			}

			if addCount > 0 {
				commit()
			}
		}
	}
}

func (wp *WorkerPool) reportStatus() {
	t := int64(time.Now().Unix() * 1000)
	statusMetrics[0].Time = t
	statusMetrics[0].Value = float64(atomic.LoadUint64(&wp.status.MetricAdd))

	statusMetrics[1].Time = t
	statusMetrics[1].Value = float64(atomic.LoadUint64(&wp.status.MetricParsed))

	statusMetrics[2].Time = t
	statusMetrics[2].Value = float64(atomic.LoadUint64(&wp.status.MetricReceived))

	if err := wp.workers[0].storage(statusMetrics); err != nil {
		log.Errorf("reportStatus err0r:%s", err.Error())
	} else {
		log.Debugf("parse seriesparsed:%f, total write:%f", statusMetrics[1].Value, statusMetrics[0].Value)
	}

	atomic.StoreUint64(&wp.status.MetricAdd, 0)
	atomic.StoreUint64(&wp.status.MetricParsed, 0)
	atomic.StoreUint64(&wp.status.MetricReceived, 0)
}
