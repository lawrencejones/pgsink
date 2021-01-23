package middleware

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"contrib.go.opencensus.io/exporter/stackdriver/propagation"
	"github.com/getsentry/sentry-go"
	sentryhttp "github.com/getsentry/sentry-go/http"
	kitlog "github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"go.opencensus.io/plugin/ochttp"
	"go.opencensus.io/trace"
	httpmw "goa.design/goa/v3/http/middleware"
	"goa.design/goa/v3/middleware"
	goa "goa.design/goa/v3/pkg"
)

// ctxKey is a private type, only constructable by this package, which helps namespace
// values we store in a context. We use a string instead of ints as they are far more
// debuggable when spewing the context.
type ctxKey string

const (
	LoggerKey = ctxKey("LoggerKey")
)

// Observe is the transport-independent observability middleware. As transport middlewares
// are run before application middlewares, we depend on the user having configured the
// transport-specific o11y middleware for this to work.
func Observe() func(e goa.Endpoint) goa.Endpoint {
	return func(e goa.Endpoint) goa.Endpoint {
		return goa.Endpoint(func(ctx context.Context, req interface{}) (interface{}, error) {
			// Start the application-level span, which is transport independent (transport
			// middleware will have started a span already)
			ctx, span := trace.StartSpan(ctx, fmt.Sprintf("%s.%s",
				ctx.Value(goa.ServiceKey), ctx.Value(goa.MethodKey)))
			defer span.End()

			// Decorate the existing logger with service and endpoint identifers, along with a
			// trace ID that can be used to lookup the request trace.
			ctx, logger := loggerSet(ctx,
				kitlog.With(LoggerFrom(ctx),
					"trace_id", span.SpanContext().TraceID,
					"endpoint_service", ctx.Value(goa.ServiceKey),
					"endpoint_method", ctx.Value(goa.MethodKey),
				))

			// Issue a log to identify the service and method
			logger.Log()

			// Call the original endpoint
			res, err := e(ctx, req)

			if err, ok := err.(interface{ StackTrace() errors.StackTrace }); ok {
				fmt.Println(err.StackTrace())
			}

			// Error handlers
			if err != nil {
				// Capture Sentry exception, if a hub is configured
				if hub := sentry.GetHubFromContext(ctx); hub != nil {
					if eventID := hub.CaptureException(err); eventID != nil {
						logger.Log("event", "capture_exception", "event_id", *eventID)
					}
				}
			}

			return res, err
		})
	}
}

// LoggerFrom retrieves the stashed logger from a request context, as put there by
// StashLogger middleware.
func LoggerFrom(ctx context.Context) kitlog.Logger {
	return ctx.Value(LoggerKey).(kitlog.Logger)
}

// loggerSet configures a logger that can be retrived with LoggerFrom.
func loggerSet(ctx context.Context, logger kitlog.Logger) (context.Context, kitlog.Logger) {
	return context.WithValue(ctx, LoggerKey, logger), logger
}

// ObserveHTTP configures a standard HTTP o11y stack. The handler wrappers are run in
// reverse order that they are applied, which means we have to 'wrap' our custom behaviour
// before any of the vendor middleware that it depends on.
func ObserveHTTP(logger kitlog.Logger) func(h http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		// Run the custom o11y handler, but only after we've applied the vendored middleware
		// below
		h = observeHTTP(logger)(h)

		// Initialises a Sentry client for the purpose of this request, if Sentry is
		// configured. This permits us to tag or scope any exceptions as appropriate.
		h = sentryhttp.New(sentryhttp.Options{
			Repanic: true, // re-throw the panic, or Goa can't handle it
		}).Handle(h)

		// Creates a request ID from incoming headers, then stashes it in the context. We can
		// use this for tagging spans etc.
		h = httpmw.RequestID(
			httpmw.UseXRequestIDHeaderOption(true),
			httpmw.XRequestHeaderLimitOption(128))(h)

		// Have OpenCensus create new traces for each request. We can't label the trace with
		// transport-agnostic RPC metadata here, as we don't have access to it, so we rely on
		// the application middleware to do it.
		h = &ochttp.Handler{
			Handler: h,
			// Use the Google propagation format, as this is what most of our services will use.
			Propagation: &propagation.HTTPFormat{},
			// Filter traces for the health check, or we start sending a lot of traces!
			IsHealthEndpoint: func(r *http.Request) bool {
				return strings.HasPrefix(r.URL.Path, "/health/check")
			},
		}

		return h
	}
}

// observeHTTP should only be called from ObserveHTTP, as that configures the required
// dependencies that need to run before we hit this handler.
func observeHTTP(logger kitlog.Logger) func(h http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Depends on httpmw.RequestID
			requestID := r.Context().Value(middleware.RequestIDKey)
			if requestID == nil {
				requestID = "unknown"
			}

			// Stash the logger into the context, so any downstream code can access it. This is
			// the initialisation of a request logger, and first time it's associated with a
			// request ID.
			ctx, logger := loggerSet(r.Context(),
				kitlog.With(logger, "request_id", requestID))
			r = r.WithContext(ctx)

			// We rely on the application middleware StashLogger to provide our logger, but we
			// won't run this until we process the request.
			started := time.Now()
			rw := httpmw.CaptureResponse(w)
			h.ServeHTTP(rw, r)

			logger.Log(
				"event", "http_request",
				"http_method", r.Method,
				"http_path", r.URL.Path,
				"http_status", rw.StatusCode,
				"http_bytes", rw.ContentLength,
				"http_duration", time.Since(started).Seconds(),
			)
		})
	}
}
