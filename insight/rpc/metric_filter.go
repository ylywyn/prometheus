package rpc

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"auto-monitor/common/log"
	"auto-monitor/common/rpc/gen-go/metrics"
)

const (
	scraperURL = "http://auto-cloud-monitor-extra.openapi.corpautohome.com/api/v1/k8smetrics/whitelist?version=%d"
	//scraperURL = "http://127.0.0.1:8080/api/v1/k8smetrics/whitelist?version=%d"
)

type MetricFilter struct {
	sync.RWMutex
	WhiteListFile string
	whiteList     map[string]bool
	stopChan      chan bool
	version       int64
}

func NewMetricFilter(whiteListFile string) *MetricFilter {
	mf := &MetricFilter{
		WhiteListFile: whiteListFile,
		stopChan:      make(chan bool),
		version:       -1,
	}
	//mf.reload()
	go mf.reloadLoop()
	return mf
}

func (filter *MetricFilter) ReloadMetricFilter() error {
	return filter.reload()
}

func (filter *MetricFilter) StopLoop() {
	select {
	case filter.stopChan <- true:
	default:
	}
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

func (filter *MetricFilter) reload() error {
	whiteList := make(map[string]bool)

	oldVersion := filter.version
	if ret, err := filter.reloadFromApi(); err != nil {
		log.Debugf("reloadFromApi error:%s", err.Error())
		temp := filter.FilterMap()
		if len(temp) > 0 {
			return nil
		}
	} else {
		if len(ret) > 0 {
			whiteList = ret
		} else {
			//没有更新
			if oldVersion == filter.version {
				return nil
			}
		}
	}

	if ret, err := filter.reloadFromFile(); err != nil {
		log.Warnf("reloadFromFile error:%s", err.Error())
	} else {
		for k, v := range ret {
			whiteList[k] = v
		}
	}

	filter.Lock()
	filter.whiteList = whiteList
	filter.Unlock()
	return nil
}

func (filter *MetricFilter) reloadLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	log.Info("MetricFilter reloadLoop loop start")
	for {
		filter.reload()
		select {
		case <-ticker.C:
		case <-filter.stopChan:
			goto exit
		}
	}

exit:
	log.Info("MetricFilter reloadLoop loop  exit")
}

func (filter *MetricFilter) reloadFromFile() (map[string]bool, error) {
	mMap := make(map[string]bool)

	var err error
	if _, err = os.Stat(filter.WhiteListFile); err != nil {
		log.Errorf("reloadMetricFilterFile doesn't exist, err: %s", err.Error())
		return nil, err
	}
	f, err := os.Open(filter.WhiteListFile)
	if err != nil {
		log.Errorf("reloadMetricFilterFile open err: %s", err.Error())
		return nil, err
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

	log.Info("file filter map:")
	for k, _ := range mMap {
		log.Infof("%s", k)
	}

	return mMap, nil
}

func (filter *MetricFilter) reloadFromApi() (map[string]bool, error) {
	mMap, err := filter.scrape()
	if err == nil && len(mMap) > 0 {
		log.Info("api filter map:")
		for k, _ := range mMap {
			log.Infof("%s", k)
		}
	}

	return mMap, err
}

type ScrapeResult struct {
	Code   int        `json:"code"`
	Result bool       `json:"result"`
	Err    string     `json:"error"`
	Data   ScrapeData `json:"data"`
}
type ScrapeData struct {
	Ver     int64           `json:"ver"`
	Scrapes map[string]bool `json:"data"`
}

func (filter *MetricFilter) scrape() (map[string]bool, error) {
	url := fmt.Sprintf(scraperURL, filter.version)
	ret := &ScrapeResult{}
	temp, err := get(url)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(temp, ret); err != nil {
		return nil, err
	}

	if ret.Data.Ver != filter.version {
		filter.version = ret.Data.Ver
		return ret.Data.Scrapes, nil
	}
	return nil, nil
}

func get(url string) ([]byte, error) {
	client := &http.Client{Timeout: 45 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return ioutil.ReadAll(resp.Body)
}
