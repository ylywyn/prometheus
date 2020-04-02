package insight

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
)

var (
	withOldLabels int32 = 0
	RelabelMap    *PodAppEnvMap
)

const (
	podLabel   = "pod"
	appLabel   = "label_deploy_record"
	appIdLabel = "label_app_id"
	envLabel   = "label_env_name"
	envIdLabel = "label_env_id"

	metricsKey = `app="%s",app_id="%s",env="%s",env_id="%s",pod="%s"`

	cacheMissTime = 180
)

func init() {
	RelabelMap = NewPodAppEnvMap()
}

type AppEnv struct {
	App   string
	Env   string
	EnvId string
	AppId string
	Pod   string

	key []byte
}

func (e *AppEnv) ToBytes() []byte {
	if len(e.key) == 0 {
		e.key = e.toBytes()
	}
	return e.key
}

func (e *AppEnv) toBytes() []byte {
	ret := fmt.Sprintf(metricsKey, e.App, e.AppId, e.Env, e.EnvId, e.Pod)
	return []byte(ret)
}

type PodAppEnvMap struct {
	sync.Mutex
	dic   map[string]*AppEnv
	count int64

	cacheMu sync.Mutex
	cache   map[string]int64
}

func NewPodAppEnvMap() *PodAppEnvMap {
	m := &PodAppEnvMap{
		dic:   make(map[string]*AppEnv),
		cache: make(map[string]int64),
	}
	return m
}

func (m *PodAppEnvMap) GetMap() map[string]*AppEnv {
	m.Lock()
	defer m.Unlock()
	return m.dic
}

func (m *PodAppEnvMap) SetMap(d map[string]*AppEnv) {
	var count int64
	m.Lock()
	m.dic = d
	m.count += 1
	count = m.count
	m.Unlock()

	if count%60 == 0 {
		m.cleanCacheCount()
	}
}

//控制pod 索引未建立起来时，key缓存是否存在（默认找3分钟， 不存在则建立原始缓存）
func (m *PodAppEnvMap) addCacheCount(pod string) bool {
	now := time.Now().Unix()

	m.cacheMu.Lock()
	defer m.cacheMu.Unlock()

	t, ok := m.cache[pod]
	if ok {
		if now-t > cacheMissTime {
			delete(m.cache, pod)
			return false
		}
	} else {
		m.cache[pod] = now
	}
	return true
}

func (m *PodAppEnvMap) cleanCacheCount() {
	m.cacheMu.Lock()
	m.cache = make(map[string]int64)
	m.cacheMu.Unlock()
}

func PodInfo(lset labels.Labels) *AppEnv {
	lbMap := lset.Map()
	podName := lbMap[podLabel]
	if len(podName) == 0 {
		podName = lbMap["pod_name"]
		if len(podName) == 0 {
			return nil
		}
	}

	app := lbMap[appLabel]
	if len(app) == 0 {
		return nil
	}

	appId := lbMap[appIdLabel]
	if len(appId) == 0 {
		return nil
	}

	env := lbMap[envLabel]
	if len(env) == 0 {
		return nil
	}

	envId := lbMap[envIdLabel]
	if len(envId) == 0 {
		return nil
	}

	info := &AppEnv{
		App:   app,
		AppId: appId,
		Env:   env,
		EnvId: envId,
		Pod:   podName,
	}
	info.key = info.toBytes()
	return info
}

//返回的bool：是否缺失索引，缺失的话，不建立缓存
func Relabel(lset labels.Labels, podInfoMap map[string]*AppEnv) ([]byte, bool) {
	podName := lset.Get(podLabel)
	if len(podName) == 0 {
		podName = lset.Get("pod_name")
		if len(podName) == 0 {
			return nil, false
		}
	}

	appEnv, ok := podInfoMap[podName]
	if !ok {
		if RelabelMap.addCacheCount(podName) {
			return nil, true
		} else {
			return nil, false
		}
	}

	return appEnv.ToBytes(), false
}

func WithOldLabels() bool {
	return atomic.LoadInt32(&withOldLabels) > 0
}
