package rpc

import (
	"errors"
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
	index      int
	stopped    bool
	series     map[string]*cacheEntry
	appender   storage.Appender
	metricChan chan []*metrics.Metric

	seriesCache *SeriesCache
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
	t := time.NewTicker(time.Duration(48) * time.Hour)
	defer t.Stop()

	for {
		select {
		case ms, ok := <-w.metricChan:
			if !ok {
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
	var err error
	for _, m := range ms {
		ce, ok := w.seriesCache.get(m.MetricKey)
		if ok {
			switch err = w.appender.AddFast(ce.lset, ce.ref, m.Time, m.Value); err {
			case nil:
				continue
			case storage.ErrNotFound:
				ok = false
			case storage.ErrOutOfOrderSample:
				log.Warnf("Out of order sample, series:", m.MetricKey)
				continue
			case storage.ErrDuplicateSampleForTimestamp:
				log.Warnf("Duplicate sample for timestamp, series:", m.MetricKey)
				continue
			case storage.ErrOutOfBounds:
				log.Warnf("Out of bounds metric, series:", m.MetricKey)
				continue
			default:
				log.Warnf("unexpected error, series: %s, err:%s", m.MetricKey, err.Error())
				continue
			}
		}

		if !ok {
			p := textparse.NewPromParser(strconv.StrToBytes(m.MetricKey))
			p.Next()

			var lset labels.Labels
			mets := p.Metric(&lset)
			hash := lset.Hash()
			if lset == nil {
				continue
			}

			var ref uint64
			ref, err = w.appender.Add(lset, m.Time, m.Value)
			switch err {
			case nil:
			case storage.ErrOutOfOrderSample:
				log.Warnf("Out of order sample, series:", m.MetricKey)
				continue
			case storage.ErrDuplicateSampleForTimestamp:
				log.Warnf("Duplicate sample for timestamp, series:", m.MetricKey)
				continue
			case storage.ErrOutOfBounds:
				log.Warnf("Out of bounds metric, series:", m.MetricKey)
				continue
			default:
				log.Warnf("unexpected error, series: %s, err:%s", m.MetricKey, err.Error())
				continue
			}

			w.seriesCache.add(mets, ref, lset, hash)
		}
	}

	return nil
}
