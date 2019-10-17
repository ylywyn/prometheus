package rpc

import (
	"context"

	"auto-insight/common/rpc/gen-go/metrics"
)

type MetricsTransferHandler struct {
	processor func(ms *metrics.Metrics) error
}

// func (h *MetricsTransferHandler) Ping(ctx context.Context) (err error) {
// 	fmt.Print("ping()\n")
// 	return nil
// }

// func (h *MetricsTransferHandler) Add(ctx context.Context, num1 int32, num2 int32) (retval17 int32, err error) {
// 	fmt.Print("add(", num1, ",", num2, ")\n")
// 	return num1 + num2, nil
// }

func (h *MetricsTransferHandler) Transfer(ctx context.Context, ms *metrics.Metrics) (r int32, err error) {
	return int32(len(ms.List)), h.processor(ms)
}
