package insight

import (
	"github.com/prometheus/prometheus/insight/rpc"

	"auto-insight/common/log"
)

var (
	Config InsightConfig

	Manager *rpc.Manager
)

type InsightConfig struct {
	RpcListenAddr       string
	RemoteRpcServerAddr string
}

func SetLog(level string) {
	log.SetLog(level, "console", "")
}

func RpcManagerRun(appender rpc.Appendable) error {
	var err error
	Manager, err = rpc.NewManager(Config.RpcListenAddr, Config.RemoteRpcServerAddr, appender)
	if err != nil {
		log.Errorf("NewRpcManager error: %s", err.Error())
		return err
	}
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
