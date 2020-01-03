package rpc

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/apache/thrift/lib/go/thrift"

	"auto-monitor/common/log"
	"auto-monitor/common/rpc/gen-go/metrics"
)

const (
	timeOut   = 5 * time.Second
	errReConn = 3
)

type Client struct {
	seq        int
	errCount   int
	reConnFlag int32
	addr       string
	transport  thrift.TTransport
	client     *metrics.MetricsTransferClient
	manager    *SendManager
}

func NewClient(manager *SendManager, i int, addr string) (*Client, error) {
	clinet := &Client{
		addr:    addr,
		seq:     i,
		manager: manager,
	}
	err := clinet.conn()
	return clinet, err
}

func (c *Client) Close() {
	if c.transport != nil {
		c.transport.Close()
		c.transport = nil
		c.client = nil
	}
}

func (c *Client) Send(ms *metrics.Metrics) error {
	if c.client == nil || c.errCount > errReConn {
		if atomic.LoadInt32(&c.reConnFlag) == 0 {
			c.reConnLoop()
		}
		return nil
	}

	d := time.Now().Add(timeOut)
	ctx, cancel := context.WithDeadline(context.Background(), d)
	defer cancel()

	var err error
	if len(c.manager.Datasource) == 0 {
		_, err = c.client.Transfer(ctx, ms)
	} else {
		_, err = c.client.TransferWithDatasource(ctx, ms, c.manager.Datasource)
	}

	if err != nil {
		c.errCount++
		log.Errorf("ds:%s rpc clent send error:%s", c.manager.name, err.Error())
	} else {
		c.errCount = 0
		//log.Debugf("rpc clent %d send metrics ok: %d", c.seq, len(ms.List))
		c.manager.metricStatus(0, uint64(len(ms.List)))
	}

	return nil
}

func (c *Client) reConnLoop() {
	log.Infof("ds:%s rpc cleint start reConnLoop: %s seq:%d", c.manager.name, c.addr, c.seq)
	atomic.AddInt32(&c.reConnFlag, 1)

	go func() {
		defer func() {
			c.errCount = 0
			atomic.AddInt32(&c.reConnFlag, -1)
			//log.Infof("rpc cleint connect ok: %s", c.addr)
		}()
		for {
			err := c.conn()
			if c.client == nil {
				if err != nil {
					log.Error(err.Error())
				}
				time.Sleep(timeOut)
				continue
			}
			break
		}
	}()
}

func (c *Client) conn() error {
	c.Close()

	transportFactory := thrift.NewTBufferedTransportFactory(8192)
	protocolFactory := thrift.NewTCompactProtocolFactory()
	transport, err := thrift.NewTSocketTimeout(c.addr, timeOut)
	if err != nil {
		if transport != nil {
			transport.Close()
		}
		return err
	}

	useTransport, err := transportFactory.GetTransport(transport)
	client := metrics.NewMetricsTransferClientFactory(useTransport, protocolFactory)
	if err := transport.Open(); err != nil {
		transport.Close()
		return fmt.Errorf("ds:%s rpc clent opening socket to %s err:%s", c.manager.name, c.addr, err)
	}
	c.client = client
	c.transport = transport

	log.Infof("ds:%s rpc clent connect to  %s  seq:%d ok", c.manager.name, c.addr, c.seq)
	return nil
}
