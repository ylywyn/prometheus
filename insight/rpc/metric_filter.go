package rpc

import (
	"bufio"
	"io"
	"os"
	"strings"

	"auto-monitor/common/log"
	"auto-monitor/common/rpc/gen-go/metrics"
)

type MetricFilter struct {
	WhiteListFile     string
	WhiteListSwitcher bool
	whiteList         map[string]bool
}

func NewMetricFilter(whiteListFile string, whiteListSwitcher bool) *MetricFilter {
	mf := &MetricFilter{
		WhiteListSwitcher: whiteListSwitcher,
		WhiteListFile:     whiteListFile,
	}
	mf.reloadMetricFilterFile()
	return mf
}

func (filter *MetricFilter) reloadMetricFilterFile() {
	mMap := make(map[string]bool)

	var err error
	if !filter.WhiteListSwitcher {
		return
	}
	if _, err = os.Stat(filter.WhiteListFile); err != nil {
		log.Infof("reloadMetricFilterFile doesn't exist, err: %s", err.Error())
		return
	}
	f, err := os.Open(filter.WhiteListFile)
	if err != nil {
		log.Infof("reloadMetricFilterFile open err: %s", err.Error())
	}
	defer f.Close()

	rd := bufio.NewReader(f)
	for {
		line, err := rd.ReadString('\n') //以'\n'为结束符读入一行

		if err != nil && io.EOF != err {
			log.Infof("reloadMetricFilterFile : %s", err.Error())
			break
		}
		if line != "" {
			line = strings.Trim(line, " ")
			line = strings.Trim(line, "\n")
			line = strings.Trim(line, "\r")
			mMap[line] = true
		}

		if io.EOF == err {
			break
		}
	}
	log.Infof("mMap : %v", mMap)
	filter.whiteList = mMap
}

func (filter *MetricFilter) ReloadMetricFilter() {
	filter.reloadMetricFilterFile()
}

func (filter *MetricFilter) GetFilteredMetrics(ms *metrics.Metrics) *metrics.Metrics {
	if filter.whiteList == nil || len(filter.whiteList) == 0 {
		return ms
	} else {
		newMs := &metrics.Metrics{List: make([]*metrics.Metric, 0)}
		for _, metric := range ms.List {
			if filter.checkIsInWhiteList(metric) {
				newMs.List = append(newMs.List, metric)
			}
		}
		return newMs
	}
}

func (filter *MetricFilter) checkIsInWhiteList(metric *metrics.Metric) bool {
	mKey := metric.MetricKey
	index := strings.Index(mKey, "{")
	if index >= 0 {
		mKey = mKey[:index]
	}
	if _, in := filter.whiteList[mKey]; in {
		return true
	} else {
		return false
	}
}
