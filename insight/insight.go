package insight

import (
	"github.com/prometheus/prometheus/insight/rpc"
	"github.com/prometheus/prometheus/storage"

	"auto-insight/common/log"
)

var (
	Config InsightConfig

	manager *rpc.Manager
)

type InsightConfig struct {
	RpcListenAddress string
}

func SetLog(level string)  {
	log.SetLog(level, "console", "")
}

func RpcManagerRun(appender storage.Appender) error {
	var err error
	manager, err = rpc.NewManager(Config.RpcListenAddress, appender)
	if err != nil {
		log.Errorf("NewRpcManager error: %s", err.Error())
		return err
	}
	if err = manager.Start(); err != nil {
		log.Errorf("RpcManagerStart error: %s", err.Error())
		return err
	}
	return nil
}

func RpcManagerStop() {
	if manager != nil {
		manager.Stop()
	}
}
