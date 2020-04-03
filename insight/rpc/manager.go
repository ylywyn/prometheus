package rpc

import (
	"runtime"
	"sync"

	"github.com/prometheus/prometheus/storage"

	"auto-monitor/common/log"
	"auto-monitor/common/rpc"
	"auto-monitor/common/rpc/gen-go/metrics"
)

type Appendable interface {
	Appender() storage.Appender
}

type Manager struct {
	sync.Mutex
	stopped      bool
	workerPool   *WorkerPool
	rpcServer    *rpc.MetricsRpcServer
	rpcSender    *rpc.SendManager
	metricFilter *MetricFilter
}

//addr:local rpc server addr, for listen
//remoteAddr: remote rpc server addr, for send
func NewManager(addr, remoteAddr, datasource string, appender Appendable) (*Manager, error) {
	rpcServer := rpc.NewMetricsRpcServer(addr)
	var rpcClient *rpc.SendManager
	if len(remoteAddr) > 8 {
		rpcClient = rpc.NewSendManager("remote", remoteAddr)
		rpcClient.Datasource = datasource
		log.Infof("remote rpc server is: %s", remoteAddr)
		log.Infof("datasource is: %s", datasource)
	}

	c := runtime.NumCPU()
	if c > 16 {
		c = c - 6
	}
	pool := NewWorkerPool(c, appender)

	m := &Manager{
		stopped:    true,
		rpcServer:  rpcServer,
		rpcSender:  rpcClient,
		workerPool: pool,
	}
	pool.manager = m
	return m, nil
}

func (m *Manager) Start() error {
	if m.stopped {
		m.stopped = false

		if m.rpcSender != nil {
			m.rpcSender.Run()
		}

		m.workerPool.Run()

		handler := rpc.MetricsTransferHandler{
			Processor:               m.workerPool.Write,
			ProcessorWithDatasource: m.workerPool.WriteWithDatasource,
		}
		if err := m.rpcServer.Run(handler); err != nil {
			log.Errorf("run rpc server error:%s", err.Error())
		}
	}
	return nil
}

func (m *Manager) Stop() {
	log.Info("rpc manager will stop")

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

		if m.rpcSender != nil {
			m.rpcSender.Stop()
		}

		if m.metricFilter != nil {
			m.metricFilter.StopLoop()
		}
	}

	log.Info("rpc manager stop!!!")
}

func (m *Manager) MetricsFilterConfig(whiteListFile string) *MetricFilter {
	mf := NewMetricFilter(whiteListFile)
	m.metricFilter = mf
	return mf
}

func (m *Manager) WriteToRemote(ms *metrics.Metrics) {
	if m.rpcSender != nil {
		//这里注释了， 在抓取端过滤了
		//mList := m.metricFilter.Filter(ms)
		if err := m.rpcSender.Send(ms); err != nil {
			log.Errorf("write to remote error:%s", err.Error())
		}
	}
}

func (m *Manager) SendRemote() bool {
	return m.rpcSender != nil
}
