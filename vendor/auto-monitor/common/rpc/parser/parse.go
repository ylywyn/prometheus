package parser

import (
	"bytes"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/model"

	"auto-monitor/common/log"
	"auto-monitor/common/rpc/gen-go/metrics"
	"auto-monitor/common/rpc/parser/expfmt"
)

const (
	TypeProtobuf = 1
	TypeText     = 2
)

var bufferPool = sync.Pool{
	New: func() interface{} {
		buf := bytes.NewBuffer(make([]byte, 128))
		return buf
	},
}

func ParseMetrics(typ int, in io.Reader, groupLabels map[string]string) (*metrics.Metrics, error) {
	if typ == TypeText {
		return ParseText(in, groupLabels)
	} else {
		return ParseProtobuf(in, groupLabels)
	}
}

func metricFamiliesForamt(mfs map[string]*dto.MetricFamily, groupLabels map[string]string) (*metrics.Metrics, error) {
	count := len(mfs)
	ms := &metrics.Metrics{List: make([]*metrics.Metric, 0, count+8)}

	t := time.Now().Unix() * 1000
	gLabelsNotYetDone := make(map[string]string, len(groupLabels))

	p := bufferPool.Get().(*bytes.Buffer)
	for _, mf := range mfs {
		sanitizeLabels(mf, groupLabels, gLabelsNotYetDone)
		metrics, err := expfmt.MetricFamilyFormat(mf, t, ms.List, p)
		if err == nil {
			ms.List = metrics
		} else {
			log.Errorf("MetricFamilyFormat error:%s", err.Error())
		}
	}
	bufferPool.Put(p)
	return ms, nil
}

func sanitizeLabels(mf *dto.MetricFamily, groupingLabels, gLabelsNotYetDone map[string]string) {
	for _, m := range mf.GetMetric() {
		for ln, lv := range groupingLabels {
			gLabelsNotYetDone[ln] = lv
		}
		hasInstanceLabel := false
		for _, lp := range m.GetLabel() {
			ln := lp.GetName()
			if lv, ok := gLabelsNotYetDone[ln]; ok {
				lp.Value = proto.String(lv)
				delete(gLabelsNotYetDone, ln)
			}
			if ln == string(model.InstanceLabel) {
				hasInstanceLabel = true
			}
			if len(gLabelsNotYetDone) == 0 && hasInstanceLabel {
				sort.Sort(labelPairs(m.Label))
				return
			}
		}
		for ln, lv := range gLabelsNotYetDone {
			m.Label = append(m.Label, &dto.LabelPair{
				Name:  proto.String(ln),
				Value: proto.String(lv),
			})
			if ln == string(model.InstanceLabel) {
				hasInstanceLabel = true
			}
			delete(gLabelsNotYetDone, ln) // To prepare map for next metric.
		}
		if !hasInstanceLabel {
			m.Label = append(m.Label, &dto.LabelPair{
				Name:  proto.String(string(model.InstanceLabel)),
				Value: proto.String(""),
			})
		}
		sort.Sort(labelPairs(m.Label))
	}

}

type labelPairs []*dto.LabelPair

func (s labelPairs) Len() int {
	return len(s)
}

func (s labelPairs) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s labelPairs) Less(i, j int) bool {
	return s[i].GetName() < s[j].GetName()
}
