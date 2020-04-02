package insight

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/prometheus/prometheus/pkg/labels"
)

var (
	withOldLabels int32
	RelabelMap    *PodAppEnvMap
)

const (
	podLabel   = "pod"
	appLabel   = "label_deploy_record"
	appIdLabel = "label_app_id"
	envLabel   = "label_env_name"
	envIdLabel = "label_env_id"

	metricsKey = `app="%s",app_id="%s",env="%s",env_id="%s",pod="%s"`
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
	dic map[string]*AppEnv
}

func NewPodAppEnvMap() *PodAppEnvMap {
	m := &PodAppEnvMap{
		dic: make(map[string]*AppEnv),
	}
	return m
}

func (m *PodAppEnvMap) GetMap() map[string]*AppEnv {
	m.Lock()
	defer m.Unlock()
	return m.dic
}

func (m *PodAppEnvMap) SetMap(d map[string]*AppEnv) {
	m.Lock()
	m.dic = d
	m.Unlock()
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

func Relabel(lset labels.Labels, podInfoMap *map[string]*AppEnv) ([]byte, bool) {
	podName := lset.Get(podLabel)
	if len(podName) == 0 {
		podName = lset.Get("pod_name")
		if len(podName) == 0 {
			return nil, false
		}
	}

	appEnv, ok := (*podInfoMap)[podName]
	if !ok {
		//重新获取字典，查看
		temp := RelabelMap.GetMap()
		*podInfoMap = temp

		appEnv, ok = temp[podName]
		if !ok {
			return nil, false
		}
	}

	return appEnv.ToBytes(), true
}

func WithOldLabels() bool {
	return atomic.LoadInt32(&withOldLabels) > 0
}
