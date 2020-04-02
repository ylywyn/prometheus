package rpc

import (
	"bufio"
	"io"
	"os"
	"strings"
	"sync"

	"auto-monitor/common/log"
	"auto-monitor/common/rpc/gen-go/metrics"
)

type MetricFilter struct {
	sync.RWMutex
	WhiteListFile string
	whiteList     map[string]bool
}

func NewMetricFilter(whiteListFile string) *MetricFilter {
	mf := &MetricFilter{
		WhiteListFile: whiteListFile,
	}
	mf.reloadMetricFilterFile()
	return mf
}

func (filter *MetricFilter) reloadMetricFilterFile() error {
	mMap := make(map[string]bool)

	var err error
	if _, err = os.Stat(filter.WhiteListFile); err != nil {
		log.Errorf("reloadMetricFilterFile doesn't exist, err: %s", err.Error())
		return err
	}
	f, err := os.Open(filter.WhiteListFile)
	if err != nil {
		log.Errorf("reloadMetricFilterFile open err: %s", err.Error())
		return err
	}
	defer f.Close()

	rd := bufio.NewReader(f)
	for {
		line, err := rd.ReadString('\n') //以'\n'为结束符读入一行
		if err != nil && io.EOF != err {
			log.Infof("reloadMetricFilterFile : %s", err.Error())
			break
		}

		line = strings.TrimSpace(line)
		line = strings.Trim(line, "\n")
		line = strings.Trim(line, "\r")
		if line != "" {
			mMap[line] = true
		}

		if io.EOF == err {
			break
		}
	}

	log.Info("filter map:")
	for k, _ := range mMap {
		log.Infof("%s", k)
	}

	filter.Lock()
	filter.whiteList = mMap
	filter.Unlock()

	return nil
}

func (filter *MetricFilter) ReloadMetricFilter() error {
	return filter.reloadMetricFilterFile()
}

func (filter *MetricFilter) Filter(ms *metrics.Metrics) *metrics.Metrics {
	filter.RLock()
	whiteList := filter.whiteList
	filter.RUnlock()

	if len(whiteList) == 0 {
		return ms
	}

	newMs := &metrics.Metrics{List: make([]*metrics.Metric, 0, len(ms.List)/3)}
	for _, metric := range ms.List {
		mKey := metric.MetricKey
		index := strings.Index(mKey, "{")
		if index >= 0 {
			mKey = mKey[:index]
		}
		if _, in := whiteList[mKey]; in {
			newMs.List = append(newMs.List, metric)
		}
	}

	//log.Infof("metrircs00000:%v", newMs.List)
	return newMs
}

func (filter *MetricFilter) FilterMap() map[string]bool {
	filter.RLock()
	whiteList := filter.whiteList
	filter.RUnlock()

	return whiteList
}
