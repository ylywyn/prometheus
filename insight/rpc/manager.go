package rpc

import (
	"runtime"
	"sync"

	"github.com/prometheus/prometheus/storage"

	"auto-insight/common/log"
	"auto-insight/common/rpc"
)

type Appendable interface {
	Appender() (storage.Appender, error)
}

type Manager struct {
	sync.Mutex
	stopped    bool
	workerPool *WorkerPool
	rpcServer  *rpc.MetricsRpcServer
}

func NewManager(addr string, appender Appendable) (*Manager, error) {
	rpcServer := rpc.NewMetricsRpcServer(addr)
	pool := NewWorkerPool(runtime.NumCPU(), appender)

	m := &Manager{
		stopped:    true,
		rpcServer:  rpcServer,
		workerPool: pool,
	}
	return m, nil
}

func (m *Manager) Start() error {
	if m.stopped {
		m.stopped = false
		m.workerPool.Run()
		if err := m.rpcServer.Run(m.workerPool.Write); err != nil {
			log.Errorf("run rpc server error:%s", err.Error())
		}
	}
	return nil
}

func (m *Manager) Stop() {
	m.Lock()
	defer m.Unlock()

	if !m.stopped {
		m.stopped = true
		if m.rpcServer != nil {
			m.rpcServer.Stop()
		}
		if m.workerPool != nil {
			m.workerPool.Stop()
		}
	}
}
