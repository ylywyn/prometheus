package rpc

import (
	"sync"
	"sync/atomic"
	"time"

	"auto-insight/common/log"
	"auto-insight/common/rpc/gen-go/metrics"
)

const metricsCount = 128

var metricsPool = sync.Pool{
	New: func() interface{} {
		return make([]*metrics.Metric, 0, metricsCount)
	},
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
	stopChan chan struct{}

	status  Status
	manager *Manager
}

func NewWorkerPool(parallel int, appender Appendable) *WorkerPool {
	p := &WorkerPool{
		stopped:  true,
		parallel: parallel,
		workers:  make([]*Worker, parallel),
		stopChan: make(chan struct{}),
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
	go wp.reportLoop()

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
		close(wp.stopChan)
		for i := 0; i < wp.parallel; i++ {
			wp.workers[i].Stop()
		}
	}
}

//将hash索引相同的 metric打到相同的sender, 使每个worker缓存一部分merics
func (wp *WorkerPool) Write(ms *metrics.Metrics) error {
	atomic.AddUint64(&wp.status.MetricReceived, uint64(len(ms.List)))
	if wp.manager.SendRemote() {
		wp.manager.WriteToRemote(ms)
	}

	msArray := make([][]*metrics.Metric, wp.parallel)
	for _, m := range ms.List {
		i := wp.workerIndex(m.MetricKey)
		if msArray[i] == nil {
			msArray[i] = metricsPool.Get().([]*metrics.Metric)
		}
		msArray[i] = append(msArray[i], m)
		if len(msArray[i]) >= metricsCount {
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

func (wp *WorkerPool) WriteWithDatasource(ms *metrics.Metrics, ds string) error {
	return wp.Write(ms)
}

//使用前8个字节，计算index
func (wp *WorkerPool) workerIndex(series string) int {
	i := 0
	l := len(series)
	for j := 0; j < l; j += 2 {
		i += int(series[j])
	}

	return i % wp.parallel
}

//统一批量Commit
func (wp *WorkerPool) Status(added int, seriesAdded int) {
	atomic.AddUint64(&wp.status.MetricAdd, uint64(added))
	atomic.AddUint64(&wp.status.MetricParsed, uint64(seriesAdded))
}

//
func (wp *WorkerPool) reportLoop() {
	t := time.NewTicker(time.Minute)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			//打印统计
			wp.reportStatus()
		case <-wp.stopChan:
			return
		}
	}
}

func (wp *WorkerPool) reportStatus() {
	var statusMetrics = []*metrics.Metric{
		&metrics.Metric{MetricKey: `prometheus_rpc_metrics{type="commited"}0`},
		&metrics.Metric{MetricKey: `prometheus_rpc_metrics{type="parsed"}0`},
		&metrics.Metric{MetricKey: `prometheus_rpc_metrics{type="received"}0`},
	}

	t := int64(time.Now().Unix() * 1000)
	statusMetrics[0].Time = t
	statusMetrics[0].Value = float64(atomic.LoadUint64(&wp.status.MetricAdd))

	statusMetrics[1].Time = t
	statusMetrics[1].Value = float64(atomic.LoadUint64(&wp.status.MetricParsed))

	statusMetrics[2].Time = t
	statusMetrics[2].Value = float64(atomic.LoadUint64(&wp.status.MetricReceived))

	if err := wp.workers[0].Storage(statusMetrics); err != nil {
		log.Errorf("reportStatus err0r:%s", err.Error())
	} else {
		log.Debugf("parse seriesparsed:%f, total write:%f, received:%f",
			statusMetrics[1].Value, statusMetrics[0].Value, statusMetrics[2].Value, )
	}
}
