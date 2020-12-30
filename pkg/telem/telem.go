package telem

import (
	"context"

	kitlog "github.com/go-kit/kit/log"
	"go.opencensus.io/trace"
)

// Logger can be used to tie logs to an on-going span. It is intended to wrap a
// trace.StartSpan call, like so:
//
//   telem.Logger(trace.StartSpan(ctx, "pkg/imports.importer.Do"))
//
// The logs will be decorated with a trace_id.
func Logger(logger kitlog.Logger) func(context.Context, *trace.Span) (context.Context, *trace.Span, kitlog.Logger) {
	return func(ctx context.Context, span *trace.Span) (context.Context, *trace.Span, kitlog.Logger) {
		if span == nil {
			return ctx, span, logger
		}

		return ctx, span, kitlog.With(logger,
			"trace_id", span.SpanContext().TraceID)
	}
}
