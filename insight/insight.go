package insight

import (
	"github.com/prometheus/prometheus/insight/rpc"

	"auto-monitor/common/log"
)

var (
	Config InsightConfig

	Manager *rpc.Manager

	MetricFilter *rpc.MetricFilter
)

type InsightConfig struct {
	RpcListenAddr       string
	RemoteRpcServerAddr string
	Datasource          string
	WhiteListFile       string
}

func SetLog(level string) {
	log.SetLog(level, "console", "")
}

func RpcManagerRun(appender rpc.Appendable) error {
	var err error
	Manager, err = rpc.NewManager(Config.RpcListenAddr, Config.RemoteRpcServerAddr, Config.Datasource, appender)
	if err != nil {
		log.Errorf("NewRpcManager error: %s", err.Error())
		return err
	}
	//metric指标的过滤配置
	MetricFilter = Manager.MetricsFilterConfig(Config.WhiteListFile)

	if err = Manager.Start(); err != nil {
		log.Errorf("RpcManagerStart error: %s", err.Error())
		return err
	}
	return nil
}

func RpcManagerStop() {
	if Manager != nil {
		Manager.Stop()
	}
}
