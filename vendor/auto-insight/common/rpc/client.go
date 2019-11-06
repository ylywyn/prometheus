package rpc

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/thrift/lib/go/thrift"

	"auto-insight/common/log"
	"auto-insight/common/rpc/gen-go/metrics"
)

const (
	timeOut   = 5 * time.Second
	errReConn = 3
)

type Client struct {
	seq       int
	errCount  int
	addr      string
	transport thrift.TTransport
	client    *metrics.MetricsTransferClient
	manager   *SendManager
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
		err := c.conn()
		if c.client == nil {
			time.Sleep(timeOut)
			return err
		}
	}

	d := time.Now().Add(timeOut)
	ctx, cancel := context.WithDeadline(context.Background(), d)
	defer cancel()

	_, err := c.client.Transfer(ctx, ms)
	if err != nil {
		c.errCount++
		log.Errorf("rpc clent send error:%s", err.Error())
	} else {
		c.errCount = 0
		//log.Debugf("rpc clent %d send metrics ok: %d", c.seq, len(ms.List))
		c.manager.metricStatus(0, uint64(len(ms.List)))
	}

	return nil
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
		return fmt.Errorf("rpc clent opening socket to %s, err:%s", c.addr, err)
	}
	c.client = client
	c.transport = transport

	log.Infof("rpc clent %d connect to %s ok", c.seq, c.addr)
	return nil
}
