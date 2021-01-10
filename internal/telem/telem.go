package telem

import (
	"context"

	kitlog "github.com/go-kit/kit/log"
	"go.opencensus.io/trace"
)

// ctxKey is a private type, only constructable by this package, which helps namespace
// values we store in a context. We use a string instead of ints as they are far more
// debuggable when spewing the context.
type ctxKey string

const (
	LoggerKey = ctxKey("LoggerKey")
)

// LoggerFrom retrieves the stashed logger from a context, as put there by loggerSet
func LoggerFrom(ctx context.Context) kitlog.Logger {
	logger, ok := ctx.Value(LoggerKey).(kitlog.Logger)
	if !ok {
		return kitlog.NewNopLogger()
	}

	return logger
}

// loggerSet configures a logger that can be retrived with LoggerFrom.
func loggerSet(ctx context.Context, logger kitlog.Logger) context.Context {
	return context.WithValue(ctx, LoggerKey, logger)
}

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
			return loggerSet(ctx, logger), span, logger
		}

		// If there's no span, we can assume no tracing is configured. No point annotating the
		// logger with a nil trace ID.
		if span == nil {
			return loggerSet(ctx, logger), span, logger
		}

		logger = kitlog.With(logger, "trace_id", span.SpanContext().TraceID)

		return loggerSet(ctx, logger), span, logger
	}
}
