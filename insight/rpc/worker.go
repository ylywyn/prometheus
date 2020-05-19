package rpc

import (
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/cespare/xxhash"
	perrs "github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/textparse"
	"github.com/prometheus/prometheus/storage"

	"auto-monitor/common/log"
	"auto-monitor/common/rpc/gen-go/metrics"
	"auto-monitor/common/strconv"
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

	w.seriesCache = NewSeriesCache(w)
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
		metricsPool.Put(ms[:0])
		return errors.New("write to worker chan timeout")
	}

	return nil
}

func (w *Worker) run() {
	t := rand.Intn(1000 * 2)
	log.Debugf("wait for: %d ms", t)
	time.Sleep(time.Duration(t) * time.Millisecond)

	//tClean := time.NewTicker(time.Minute)
	tClean := time.NewTicker(time.Duration(12) * time.Hour)
	defer tClean.Stop()

	interval := 5
	tCommit := time.NewTicker(time.Duration(interval) * time.Second)
	defer tCommit.Stop()

	count := 0
	const commitCount = 2048

	app := w.appender.Appender()
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

		count = 0
		app = w.appender.Appender()
	}

	log.Infof("worker %d run...", w.index)

	for {
		select {
		case ms, ok := <-w.metricChan:
			if !ok {
				log.Infof("worker %d quit", w.index)
				return
			}

			add, err := w.storage(ms, app)
			if err != nil {
				log.Errorf("write to tsdb error:%s", err.Error())
			}

			count += add
			if count > commitCount {
				commit()
			}

			metricsPool.Put(ms[:0])
		case <-tCommit.C:
			if count > 0 {
				commit()
			}

		case <-tClean.C:
			go w.seriesCache.clearTimeout()
		}
	}
}

func (w *Worker) storage(ms []*metrics.Metric, app storage.Appender) (int, error) {
	added := 0
	seriesAdded := 0
	var errRet error
	var errParse error

	now := time.Now()
	t := now.UnixNano() / 1e6
	tSec := now.Unix()
	const diffTime = 10 * 1000
	for _, m := range ms {
		if math.Abs(float64(t-m.Time)) < diffTime {
			m.Time = t
		}

		var ok bool
		var hash uint64
		var ce *cacheEntry
		if len(m.MetricKey) > 128 {
			hash = xxhash.Sum64String(m.MetricKey)
			ce, ok = w.seriesCache.getFast(hash, tSec)
		} else {
			ce, ok = w.seriesCache.get(m.MetricKey, tSec)
		}

		if ok {
			//if err := app.AddFast(ce.lset, ce.ref, m.Time, m.Value); err != nil {
			if err := app.AddFast(ce.ref, m.Time, m.Value); err != nil {
				if perrs.Is(err, storage.ErrNotFound) {
					ok = false
				} else {
					//log.Errorf("appender.AddFast error:%s", err.Error())
					errRet = fmt.Errorf("appender.AddFast: %s 170", err.Error())
					continue
				}
			} else {
				added++
				continue
			}
		}

		if !ok {
			p := textparse.NewPromParser(strconv.StrToBytes(m.MetricKey))
			_, err := p.Next()
			if err != nil {
				if err != io.EOF && errParse == nil {
					errParse = fmt.Errorf("Parser error:%s, metiric:%s", err.Error(), m.MetricKey)
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
				errRet = fmt.Errorf("appender.Add: %s. 198", err.Error())
				continue
			}

			added++
			seriesAdded++
			if hash > 0 {
				w.seriesCache.addFast(hash, ref, lset, tSec)
			} else {
				w.seriesCache.add(m.MetricKey, ref, lset, tSec)
			}
		}
	}

	if errParse != nil {
		log.Error(errParse.Error())
	}

	log.Debugf("worker: %d add:%d", w.index, added)
	if added > 0 {
		w.pool.Status(added, seriesAdded)
	}

	return added, errRet
}
