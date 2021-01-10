package telem

import (
	"context"

	kitlog "github.com/go-kit/kit/log"
	"go.opencensus.io/trace"
)

// Logger can be used to tie logs to an on-going span. It is intended to wrap a
// trace.StartSpan call, like so:
//
//	telem.Logger(ctx, logger)(trace.StartSpan(ctx, "pkg/imports.importer.Do"))
//
// The logs will be decorated with a trace_id. Root context is provided to avoid doubly
// annotating the trace ID onto the same logger.
func Logger(rootCtx context.Context, logger kitlog.Logger) func(context.Context, *trace.Span) (context.Context, *trace.Span, kitlog.Logger) {
	return func(ctx context.Context, span *trace.Span) (context.Context, *trace.Span, kitlog.Logger) {
		// If the root context already has a trace, assume our logger has been tagged and do
		// nothing.
		if trace.FromContext(rootCtx) != nil {
			return ctx, span, logger
		}

		// If there's no span, we can assume no tracing is configured. No point annotating the
		// logger with a nil trace ID.
		if span == nil {
			return ctx, span, logger
		}

		return ctx, span, kitlog.With(logger,
			"trace_id", span.SpanContext().TraceID)
	}
}
