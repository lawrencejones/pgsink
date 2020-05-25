package generic

import (
	"context"

	kitlog "github.com/go-kit/kit/log"
	"github.com/lawrencejones/pgsink/pkg/changelog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opencensus.io/trace"
)

var (
	sinkInsertDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "pgsink_sink_insert_duration_seconds",
			Help:    "Distribution of time spent issuing inserts, by route (if applicable)",
			Buckets: prometheus.ExponentialBuckets(0.125, 2, 12), // 0.125 -> 512s
		},
		[]string{"route"},
	)
	sinkInsertBatchSize = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "pgsink_sink_insert_batch_size",
			Help:    "Distribution of insert batch sizes",
			Buckets: prometheus.ExponentialBuckets(1, 2, 13), // 1 -> 8192
		},
		[]string{"route"},
	)
)

type instrumentedInserter struct {
	Inserter
	logger                     kitlog.Logger
	route                      string
	durationSeconds, batchSize prometheus.ObserverVec
}

// NewInstrumentedInserter wraps an existing synchronous inserter, causing every insert to
// be logged, capture batch size and duration in metrics, and create new spans.
func NewInstrumentedInserter(logger kitlog.Logger, route Route, i Inserter) Inserter {
	labels := prometheus.Labels(map[string]string{"route": string(route)})
	logger = kitlog.With(logger, "route", string(route))

	return &instrumentedInserter{
		Inserter:        i,
		logger:          logger,
		route:           string(route),
		durationSeconds: sinkInsertDurationSeconds.MustCurryWith(labels),
		batchSize:       sinkInsertBatchSize.MustCurryWith(labels),
	}
}

func (i *instrumentedInserter) Insert(ctx context.Context, modifications []*changelog.Modification) (count int, lsn *uint64, err error) {
	ctx, span := trace.StartSpan(ctx, "pkg/sinks/generic.Inserter.Insert()")
	defer span.End()

	batchSize := len(modifications)
	span.AddAttributes(
		trace.StringAttribute("route", i.route),
		trace.Int64Attribute("batch_size", int64(batchSize)),
	)

	defer prometheus.NewTimer(prometheus.ObserverFunc(func(v float64) {
		i.logger.Log("event", "insert", "duration", v, "batch_size", batchSize, "count", count, "lsn", lsn, "error", err)
		i.durationSeconds.WithLabelValues().Observe(v)
		i.batchSize.WithLabelValues().Observe(float64(batchSize))
	})).ObserveDuration()

	return i.Inserter.Insert(ctx, modifications)
}
