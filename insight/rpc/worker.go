package rpc

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
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
	appender    Appendable
	metricChan  chan []*metrics.Metric

	pool *WorkerPool
}

func NewWorker(i int, appender Appendable) *Worker {
	w := &Worker{
		index:      i,
		stopped:    true,
		appender:   appender,
		metricChan: make(chan []*metrics.Metric, 512),
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
		return errors.New("write to worker chan timeout")
	}

	return nil
}

func (w *Worker) run() {
	t := rand.Intn(1000 * 2)
	log.Debugf("wait for: %d ms", t)
	time.Sleep(time.Duration(t) * time.Millisecond)

	tClean := time.NewTicker(time.Duration(48) * time.Hour)
	defer tClean.Stop()

	interval := 3
	tCommit := time.NewTicker(time.Duration(interval) * time.Second)
	defer tCommit.Stop()

	errCount := 0
	var err error
	var app storage.Appender
	for {
		app, err = w.appender.Appender()
		if err != nil {
			log.Errorf("w.appender.Appender error:%s", err.Error())

			errCount ++
			if errCount >= 5 {
				panic(err)
			}
			time.Sleep(2 * time.Second)
			continue
		} else {
			break
		}
	}

	commit := func() {
		//防止同一时间提交
		if log.IsDebug() {
			begin := time.Now().UnixNano()
			defer func() {
				log.Debugf("p.appender.Commit time: %d", (time.Now().UnixNano()-begin)/1e6)
			}()
		}

		if err := app.Commit(); err != nil {
			log.Errorf("wp.appender.Commit error: %s", err.Error())
		}

		app, err = w.appender.Appender()
		if err != nil {
			log.Errorf("w.appender.Appender error:%s", err.Error())
			panic(err)
		}
	}

	log.Infof("worker %d run...", w.index)

	for {
		select {
		case ms, ok := <-w.metricChan:
			if !ok {
				log.Infof("worker %d quit", w.index)
				return
			}

			err := w.storage(ms, app)
			if err != nil {
				log.Errorf("write to tsdb error:%s", err.Error())
			}

		case <-tCommit.C:
			commit()

		case <-tClean.C:
			w.seriesCache.clear()
		}
	}
}

func (w *Worker) storage(ms []*metrics.Metric, app storage.Appender) error {
	added := 0
	seriesAdded := 0
	var errRet error

	for _, m := range ms {
		ce, ok := w.seriesCache.get(m.MetricKey)
		if ok {
			if err := app.AddFast(ce.lset, ce.ref, m.Time, m.Value); err != nil {
				if err == storage.ErrNotFound {
					ok = false
				} else {
					//log.Errorf("appender.AddFast error:%s", err.Error())
					errRet = fmt.Errorf("appender.AddFast: %s", err.Error())
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
			if lset == nil {
				continue
			}

			ref, err := app.Add(lset, m.Time, m.Value)
			if err != nil {
				//log.Errorf("appender.Add error:%s", err.Error())
				errRet = fmt.Errorf("appender.Add: %s", err.Error())
				continue
			}

			added ++
			seriesAdded ++
			w.seriesCache.add(m.MetricKey, ref, lset)
		}
	}

	metricsPool.Put(ms[:0])

	log.Debugf("worker: %d add:%d", w.index, added)
	if added > 0 {
		w.pool.Status(added, seriesAdded)
	}
	return errRet
}
