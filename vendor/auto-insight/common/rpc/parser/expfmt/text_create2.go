package expfmt

import (
	"bytes"
	"fmt"
	"math"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/model"

	"auto-insight/common/rpc/gen-go/metrics"
)

func MetricFamilyFormat(in *dto.MetricFamily, t int64, ms []*metrics.Metric, b *bytes.Buffer) ([]*metrics.Metric, error) {
	if len(in.Metric) == 0 {
		return ms, fmt.Errorf("MetricFamily has no metrics: %s", in)
	}
	name := in.GetName()
	if name == "" {
		return ms, fmt.Errorf("MetricFamily has no name: %s", in)
	}

	metricType := in.GetType()
	for _, metric := range in.Metric {
		switch metricType {
		case dto.MetricType_COUNTER:
			if metric.Counter == nil {
				return ms, fmt.Errorf("expected counter in metric %s %s", name, metric)
			}
			key, err := makeMetricKey(b, name, "", metric, "", 0)
			if err != nil {
				return ms, fmt.Errorf("expected counter in metric %s %s", name, metric)
			}
			ms = append(ms, makeMetric(key, metric.Counter.GetValue(), t))

		case dto.MetricType_GAUGE:
			if metric.Gauge == nil {
				return ms, fmt.Errorf("expected gauge in metric %s %s", name, metric)
			}
			key, err := makeMetricKey(b, name, "", metric, "", 0)
			if err != nil {
				return ms, fmt.Errorf("expected gauge in metric %s %s", name, metric)
			}
			ms = append(ms, makeMetric(key, metric.Gauge.GetValue(), t))

		case dto.MetricType_UNTYPED:
			if metric.Untyped == nil {
				return ms, fmt.Errorf("expected untyped in metric %s %s", name, metric)
			}
			key, err := makeMetricKey(b, name, "", metric, "", 0)
			if err != nil {
				return ms, fmt.Errorf("expected untyped in metric %s %s", name, metric)
			}
			ms = append(ms, makeMetric(key, metric.Untyped.GetValue(), t))

		case dto.MetricType_SUMMARY:
			if metric.Summary == nil {
				return ms, fmt.Errorf("expected summary in metric %s %s", name, metric)
			}
			for _, q := range metric.Summary.Quantile {
				key, err := makeMetricKey(b, name, "", metric, model.QuantileLabel, q.GetQuantile())
				if err != nil {
					return ms, err
				}
				ms = append(ms, makeMetric(key, q.GetValue(), t))
			}
			// Summary
			key, err := makeMetricKey(b, name, "_sum", metric, "", 0)
			if err != nil {
				return ms, err
			}
			ms = append(ms, makeMetric(key, metric.Summary.GetSampleSum(), t))

			key, err = makeMetricKey(b, name, "_count", metric, "", 0)
			if err != nil {
				return ms, err
			}
			ms = append(ms, makeMetric(key, float64(metric.Summary.GetSampleCount()), t))

		case dto.MetricType_HISTOGRAM:
			if metric.Histogram == nil {
				return ms, fmt.Errorf("expected histogram in metric %s %s", name, metric)
			}
			infSeen := false
			for _, bucket := range metric.Histogram.Bucket {
				key, err := makeMetricKey(b, name, "_bucket", metric, model.BucketLabel, bucket.GetUpperBound())
				if err != nil {
					return ms, err
				}
				ms = append(ms, makeMetric(key, float64(bucket.GetCumulativeCount()), t))

				if math.IsInf(bucket.GetUpperBound(), +1) {
					infSeen = true
				}
			}
			if !infSeen {
				key, err := makeMetricKey(b, name, "_bucket", metric, model.BucketLabel, math.Inf(+1))
				if err != nil {
					return ms, err
				}
				ms = append(ms, makeMetric(key, float64(metric.Histogram.GetSampleCount()), t))
			}
			key, err := makeMetricKey(b, name, "_sum", metric, "", 0)
			if err != nil {
				return ms, err
			}
			ms = append(ms, makeMetric(key, float64(metric.Histogram.GetSampleSum()), t))

			key, err = makeMetricKey(b, name, "_count", metric, "", 0)
			if err != nil {
				return ms, err
			}
			ms = append(ms, makeMetric(key, float64(metric.Histogram.GetSampleCount()), t))

		default:
			return ms, fmt.Errorf("unexpected type in metric %s %s", name, metric)
		}

	}
	return ms, nil
}

func makeMetricKey(w *bytes.Buffer, name, suffix string, metric *dto.Metric,
	additionalLabelName string, additionalLabelValue float64) ([]byte, error) {

	w.Reset()
	_, err := w.WriteString(name)
	if err != nil {
		return nil, err
	}
	if suffix != "" {
		_, err = w.WriteString(suffix)
		if err != nil {
			return nil, err
		}
	}
	_, err = writeLabelPairs(
		w, metric.Label, additionalLabelName, additionalLabelValue,
	)
	if err != nil {
		return nil, err
	}

	w.WriteByte('0')
	return w.Bytes(), nil
}

func makeMetric(key []byte, value float64, time int64) *metrics.Metric {
	return &metrics.Metric{
		MetricKey: string(key),
		Time:      time,
		Value:     value,
	}
}
