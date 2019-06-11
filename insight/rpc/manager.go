package rpc

import (
	"runtime"
	"sync"

	"github.com/prometheus/prometheus/storage"

	"auto-insight/common/log"
	"auto-insight/common/rpc"
)

type Manager struct {
	sync.Mutex
	stopped   bool
	rpcServer *rpc.MetricsRpcServer
	pool      *WorkerPool
}

func NewManager(addr string, appender storage.Appender) (*Manager, error) {
	rpcServer := rpc.NewMetricsRpcServer(addr)
	pool := NewWorkerPool(runtime.NumCPU(), appender)

	m := &Manager{
		stopped:   true,
		rpcServer: rpcServer,
		pool:      pool,
	}
	return m, nil
}

func (m *Manager) Start() error {
	if m.stopped {
		m.pool.Run()
		if err := m.rpcServer.Run(m.pool.Write); err != nil {
			log.Errorf("run rpc server error:%s", err.Error())
		}
	}
	return nil
}

func (m *Manager) Stop() {
	m.Lock()
	defer m.Unlock()

	if !m.stopped {
		if m.rpcServer != nil {
			m.rpcServer.Stop()
		}
		if m.pool != nil {
			m.pool.Stop()
		}
	}
}
