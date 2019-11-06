package rpc

import (
	"errors"
	"time"

	"auto-insight/common/log"
	"auto-insight/common/rpc/gen-go/metrics"
	"auto-insight/common/strconv"
)

type bufferClient struct {
	client        *Client
	buf           sizeBuffer
	lastFlushTime int64
}

type sizeBuffer struct {
	buf  []*metrics.Metric
	size int
}

func (b *sizeBuffer) add(m *metrics.Metric) {
	b.buf[b.size] = m
	b.size += 1
}

func newBufferClient(manager *SendManager, i int, addr string) (*bufferClient, error) {
	c, err := NewClient(manager, i, addr)
	if err != nil {
		return nil, err
	}
	buf := sizeBuffer{
		buf:  make([]*metrics.Metric, batchNumbers),
		size: 0,
	}

	ret := &bufferClient{
		client: c,
		buf:    buf,
	}
	return ret, nil
}

func (c *bufferClient) tryToSend(m *metrics.Metric) {
	c.buf.add(m)
	if c.buf.size >= batchNumbers {
		c.send()
	}
}
func (c *bufferClient) send() {
	ms := &metrics.Metrics{List: c.buf.buf}
	if err := c.client.Send(ms); err != nil {
		log.Errorf("send to judge error:%s", err.Error())
	}
	c.buf.size = 0
	c.lastFlushTime = time.Now().Unix()
}

func (c *bufferClient) flush(now int64) {
	if now-c.lastFlushTime >= 6 && c.buf.size > 0 {
		c.send()
	}
}

type HashClient struct {
	clients  []*bufferClient
	dataChan chan *metrics.Metrics
	addrs    []string
}

func NewHashClient(manager *SendManager, hashAddrs []string) (*HashClient, error) {
	count := len(hashAddrs)
	clients := make([]*bufferClient, 0, count)
	for i := 0; i < count; i++ {
		client, err := newBufferClient(manager, i, hashAddrs[i])
		if err != nil {
			return nil, err
		}
		clients = append(clients, client)
	}

	hc := &HashClient{
		clients:  clients,
		addrs:    hashAddrs,
		dataChan: make(chan *metrics.Metrics, batchNumbers),
	}

	go hc.sendLoop()
	return hc, nil
}

func (hc *HashClient) Send(ms *metrics.Metrics) error {
	select {
	case hc.dataChan <- ms:
	default:
		return errors.New("sender data channel is full")
	}
	return nil
}

func (hc *HashClient) sendLoop() error {
	t := time.NewTicker(time.Duration(5) * time.Second)
	defer t.Stop()
	for {
		select {
		case ms, ok := <-hc.dataChan:
			if !ok {
				log.Debug("send loop quit")
				return nil
			}

			if ms != nil {
				for _, m := range ms.List {
					pos := Murmur3(strconv.StrToBytes(m.MetricKey))
					hc.clients[pos].tryToSend(m)
				}
			}
		case <-t.C:
			hc.flush()
		}
	}

	log.Infof("judge sender loop seq is quit")
	return nil
}

func (hc *HashClient) flush() {
	now := time.Now().Unix()
	for i := 0; i < len(hc.addrs); i++ {
		hc.clients[i].flush(now)
	}
}
