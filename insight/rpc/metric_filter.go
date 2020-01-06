package rpc

import (
	"auto-monitor/common/log"
	"auto-monitor/common/rpc/gen-go/metrics"
	"bufio"
	"io"
	"os"
	"strings"
)

type MetricFilter struct {
	WhiteListFile     string
	WhiteListSwitcher bool
	whiteList         map[string]bool
}

func NewMetricFilter(whiteListFile string, whiteListSwitcher bool) *MetricFilter {
	var err error
	mMap := make(map[string]bool)
	mf := &MetricFilter{
		WhiteListSwitcher: whiteListSwitcher,
		WhiteListFile:     whiteListFile,
	}
	if whiteListSwitcher {
		if _, err = os.Stat(whiteListFile); err != nil {
			log.Infof("whiteListFile doesn't exist, err: %s", err.Error())
			goto QUIT
		}
		f, err := os.Open(whiteListFile)
		if err != nil {
			panic(err)
		}
		defer f.Close()

		rd := bufio.NewReader(f)
		for {
			line, err := rd.ReadString('\n') //以'\n'为结束符读入一行

			if err != nil {
				log.Infof("ReloadMetricFilter : %s", err.Error())
				break
			}
			if io.EOF == err {
				break
			}
			line = strings.Trim(line, " ")
			line = strings.Trim(line, "\n")
			log.Infof("Metric : %s", line)
			mMap[line] = true
		}
		log.Infof("mMap : %v", mMap)
	}
QUIT:
	mf.whiteList = mMap
	return mf
}

func (filter *MetricFilter) ReloadMetricFilter() {
	var err error
	mMap := make(map[string]bool)
	if filter.WhiteListSwitcher {

		if _, err = os.Stat(filter.WhiteListFile); err != nil {
			log.Infof("whiteListFile doesn't exist, err: %s", err.Error())
			goto QUIT
		}

		f, err := os.Open(filter.WhiteListFile)
		if err != nil {
			panic(err)
		}
		defer f.Close()

		rd := bufio.NewReader(f)
		for {
			line, err := rd.ReadString('\n') //以'\n'为结束符读入一行

			if err != nil {
				log.Infof("ReloadMetricFilter : %s", err.Error())
				break
			}
			if io.EOF == err {
				break
			}
			line = strings.Trim(line, " ")
			log.Infof("Metric : %s", line)
			mMap[line] = true
		}
		log.Infof("mMap : %v", mMap)
	}
QUIT:
	filter.whiteList = mMap
}

func (filter *MetricFilter) GetFilteredMetrics(ms *metrics.Metrics) *metrics.Metrics {
	if len(filter.whiteList) == 0 {
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
