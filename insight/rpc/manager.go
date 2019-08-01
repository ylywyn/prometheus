package rpc

import (
	"runtime"
	"sync"

	"github.com/prometheus/prometheus/storage"

	"auto-insight/common/log"
	"auto-insight/common/rpc"
	"auto-insight/common/rpc/gen-go/metrics"
)

type Appendable interface {
	Appender() (storage.Appender, error)
}

type Manager struct {
	sync.Mutex
	stopped    bool
	workerPool *WorkerPool
	rpcServer  *rpc.MetricsRpcServer
	rpcClient  *rpc.SendManager
}

//addr:local rpc server addr, for listen
//remoteAddr: remote rpc server addr, for send
func NewManager(addr, remoteAddr string, appender Appendable) (*Manager, error) {
	rpcServer := rpc.NewMetricsRpcServer(addr)
	var rpcClient *rpc.SendManager
	if len(remoteAddr) > 8 {
		rpcClient = rpc.NewSendManager("remote", remoteAddr)
		log.Infof("remote prometheus server is: %s", remoteAddr)
	}

	pool := NewWorkerPool(runtime.NumCPU(), appender)

	m := &Manager{
		stopped:    true,
		rpcServer:  rpcServer,
		rpcClient:  rpcClient,
		workerPool: pool,
	}
	pool.manager = m
	return m, nil
}

func (m *Manager) Start() error {
	if m.stopped {
		m.stopped = false

		if m.rpcClient != nil {
			m.rpcClient.Run()
		}

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

		if m.rpcClient != nil {
			m.rpcClient.Stop()
		}
	}
}

func (m *Manager) WriteToRemote(ms *metrics.Metrics) {
	if m.rpcClient != nil {
		if err := m.rpcClient.Send(ms); err != nil {
			log.Errorf("write to remote error:%s", err.Error())
		}
	}
}

func (m *Manager) SendRemote() bool {
	return m.rpcClient != nil
}
