package rpc

import (
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"

	"auto-insight/common/log"
	"auto-insight/common/rpc/gen-go/metrics"
)

type Sender struct {
	sync.Mutex
	seq     int
	stopped bool
	addr    string

	flushInterval int
	buff          []*metrics.Metric
	dataChan      chan *metrics.Metrics
	manager       *SendManager
}

func NewSender(manager *SendManager, i, flushInterval int, addr string) *Sender {
	sc := &Sender{
		seq:           i,
		addr:          addr,
		stopped:       true,
		flushInterval: flushInterval,
		buff:          make([]*metrics.Metric, 0, batchNumbers*2),
		dataChan:      make(chan *metrics.Metrics, batchNumbers*4),
		manager:       manager,
	}
	return sc
}

func (s *Sender) Start() {
	s.Lock()
	defer s.Unlock()

	if s.stopped {
		s.stopped = false
		go s.sendLoop()
	}
}

func (s *Sender) Send(ms *metrics.Metrics) error {
	select {
	case s.dataChan <- ms:
	default:
		return errors.New("sender data channel is full")
	}
	return nil
}

func (s *Sender) sendLoop() {
	client, err := NewClient(s.manager, s.seq, s.addr)
	if err != nil {
		log.Errorf(fmt.Sprintf("sender create client error:%s", err.Error()))
	}
	defer client.Close()

	var hashClient *HashClient
	if len(s.manager.HashAddr) > 0 {
		hashClient, err = NewHashClient(s.manager, s.manager.HashAddr)
		if err != nil {
			log.Errorf("NewHashClient error :%s", err.Error())
		}
	}

	log.Infof("rpc[%s] sender loop seq: %d is runing", s.addr, s.seq)

	t := time.NewTicker(time.Duration(s.flushInterval) * time.Second)
	defer t.Stop()
	for {
		select {
		case d, ok := <-s.dataChan:
			if !ok {
				log.Debug("send loop quit")
				return
			}

			if len(d.List) > 64 {
				if err := client.Send(d); err != nil {
					log.Error(fmt.Sprintf("tsdb client write error: %s", err.Error()))
				}
			} else {
				s.buff = append(s.buff, d.List...)
				if len(s.buff) >= batchNumbers {
					if err := client.Send(&metrics.Metrics{List: s.buff}); err != nil {
						log.Error(fmt.Sprintf("tsdb client write error: %s", err.Error()))
					}
					s.buff = s.buff[:0]
				}
			}

			if hashClient != nil {
				if err := hashClient.Send(d); err != nil {
					log.Errorf("hashClient send error :%s", err.Error())
				}
			}

		case <-t.C:
			if !s.stopped && len(s.buff) > 0 {
				if err := client.Send(&metrics.Metrics{List: s.buff}); err != nil {
					log.Error(fmt.Sprintf("tsdb client write error: %s", err.Error()))
				}
				s.buff = s.buff[:0]
			}
		}
	}

	log.Infof("rpc[%s] sender loop seq: %d is quit", s.addr, s.seq)
}

func (s *Sender) Stop() {
	s.Lock()
	defer s.Unlock()
	if s.stopped {
		return
	}

	s.stopped = true
	close(s.dataChan)

	log.Infof("rpc[%s] sender loop seq: %d is Stop", s.addr, s.seq)
}
