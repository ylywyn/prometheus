package rpc

import (
	"errors"
	"io"
	"sync"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/textparse"
	"github.com/prometheus/prometheus/storage"

	"auto-insight/common/log"
	"auto-insight/common/rpc/gen-go/metrics"
	"auto-insight/common/strconv"
)

type Worker struct {
	sync.Mutex
	index   int
	stopped bool

	seriesCache *SeriesCache
	appender    storage.Appender
	metricChan  chan []*metrics.Metric

	pool *WorkerPool
}

func NewWorker(i int, appender storage.Appender) *Worker {
	w := &Worker{
		index:      i,
		stopped:    true,
		appender:   appender,
		metricChan: make(chan []*metrics.Metric, 256),
	}

	w.seriesCache = NewSeriesCache()
	return w
}

func (w *Worker) Run() {
	w.Lock()
	defer w.Unlock()

	if w.stopped {
		w.stopped = false
		go w.run()
	}
}

func (w *Worker) Stop() {
	log.Infof("worker %d Stop", w.index)

	w.Lock()
	defer w.Unlock()
	if !w.stopped {
		w.stopped = true
		close(w.metricChan)
	}
}

func (w *Worker) Storage(ms []*metrics.Metric) error {
	select {
	case w.metricChan <- ms:
	default:
		return errors.New("write to worker chan timeout!")
	}

	return nil
}

func (w *Worker) run() {
	log.Infof("worker %d run...", w.index)

	t := time.NewTicker(time.Duration(48) * time.Hour)
	defer t.Stop()

	for {
		select {
		case ms, ok := <-w.metricChan:
			if !ok {
				log.Infof("worker %d quit", w.index)
				return
			}

			err := w.storage(ms)
			if err != nil {
				log.Errorf("write to tsdb error:%s", err.Error())
			}
		case <-t.C:
			w.seriesCache.clear()
		}
	}
}

func (w *Worker) storage(ms []*metrics.Metric) error {
	added := 0
	seriesAdded := 0
	
	for _, m := range ms {
		ce, ok := w.seriesCache.get(m.MetricKey)
		if ok {
			if err := w.appender.AddFast(ce.lset, ce.ref, m.Time, m.Value); err != nil {
				if err == storage.ErrNotFound {
					ok = false
				} else {
					log.Errorf("appender.AddFast error:%s", err.Error())
					continue
				}
			} else {
				added ++
				continue
			}
		}

		if !ok {
			p := textparse.NewPromParser(strconv.StrToBytes(m.MetricKey))
			_, err := p.Next()
			if err != nil {
				if err != io.EOF {
					log.Errorf("NewPromParser error:%s", err.Error())
				}
				continue
			}

			var lset labels.Labels
			p.Metric(&lset)
			hash := lset.Hash()
			if lset == nil {
				continue
			}

			ref, err := w.appender.Add(lset, m.Time, m.Value)
			if err != nil {
				log.Errorf("appender.Add error:%s", err.Error())
				continue
			}

			added ++
			seriesAdded ++
			w.seriesCache.add(m.MetricKey, ref, lset, hash)
		}
	}

	metricsPool.Put(ms)
	if added > 0 {
		w.pool.AppenderCommit(added, seriesAdded)
	}
	return nil
}
