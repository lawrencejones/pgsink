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
	loggerKey = ctxKey("loggerKey")
)

// LoggerFrom retrieves the stashed logger from a context, as put there by WithLogger
func LoggerFrom(ctx context.Context, keyvals ...interface{}) kitlog.Logger {
	logger, ok := ctx.Value(loggerKey).(kitlog.Logger)
	if !ok {
		return kitlog.NewNopLogger()
	}

	if len(keyvals) > 0 {
		logger = kitlog.With(logger, keyvals...)
	}

	return logger
}

// WithLogger configures a logger that can be retrived with LoggerFrom.
func WithLogger(ctx context.Context, logger kitlog.Logger, keyvals ...interface{}) (context.Context, kitlog.Logger) {
	if len(keyvals) > 0 {
		logger = kitlog.With(logger, keyvals...)
	}

	return context.WithValue(ctx, loggerKey, logger), logger
}

// StartSpan extends the opencensus method so it stashes a logger into the context, and
// annotates the logger with the current trace ID. If a logger was never provided, we use
// a no-op logger.
func StartSpan(ctx context.Context, name string, opts ...trace.StartOption) (context.Context, *trace.Span, kitlog.Logger) {
	logger := LoggerFrom(ctx)
	parent := trace.FromContext(ctx)

	ctx, span := trace.StartSpan(ctx, name, opts...)

	// If there's no span, we can assume no tracing is configured. No point annotating the
	// logger with a nil trace ID, so return everything unchanged.
	if span == nil {
		return ctx, span, logger
	}

	// If the original context was already being traced, assume our logger has been tagged
	// and do nothing.
	if parent != nil {
		return ctx, span, logger
	}

	ctx, logger = WithLogger(ctx, logger, "trace_id", span.SpanContext().TraceID)

	return ctx, span, logger
}
