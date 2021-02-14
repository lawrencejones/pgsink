// Code generated by goa v3.2.6, DO NOT EDIT.
//
// Web HTTP server
//
// Command:
// $ goa gen github.com/lawrencejones/pgsink/api/design -o api

package server

import (
	"context"
	"net/http"
	"path"
	"strings"

	web "github.com/lawrencejones/pgsink/api/gen/web"
	goahttp "goa.design/goa/v3/http"
)

// Server lists the Web service endpoint HTTP handlers.
type Server struct {
	Mounts []*MountPoint
}

// ErrorNamer is an interface implemented by generated error structs that
// exposes the name of the error as defined in the design.
type ErrorNamer interface {
	ErrorName() string
}

// MountPoint holds information about the mounted endpoints.
type MountPoint struct {
	// Method is the name of the service method served by the mounted HTTP handler.
	Method string
	// Verb is the HTTP method used to match requests to the mounted handler.
	Verb string
	// Pattern is the HTTP request path pattern used to match requests to the
	// mounted handler.
	Pattern string
}

// New instantiates HTTP handlers for all the Web service endpoints using the
// provided encoder and decoder. The handlers are mounted on the given mux
// using the HTTP verb and path defined in the design. errhandler is called
// whenever a response fails to be encoded. formatter is used to format errors
// returned by the service methods prior to encoding. Both errhandler and
// formatter are optional and can be nil.
func New(
	e *web.Endpoints,
	mux goahttp.Muxer,
	decoder func(*http.Request) goahttp.Decoder,
	encoder func(context.Context, http.ResponseWriter) goahttp.Encoder,
	errhandler func(context.Context, http.ResponseWriter, error),
	formatter func(err error) goahttp.Statuser,
) *Server {
	return &Server{
		Mounts: []*MountPoint{
			{"web/build", "GET", "/web"},
		},
	}
}

// Service returns the name of the service served.
func (s *Server) Service() string { return "Web" }

// Use wraps the server handlers with the given middleware.
func (s *Server) Use(m func(http.Handler) http.Handler) {
}

// Mount configures the mux to serve the Web endpoints.
func Mount(mux goahttp.Muxer) {
	MountWebBuild(mux, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upath := path.Clean(r.URL.Path)
		rpath := upath
		if strings.HasPrefix(upath, "/web") {
			rpath = upath[4:]
		}
		http.ServeFile(w, r, path.Join("web/build", rpath))
	}))
}

// MountWebBuild configures the mux to serve GET request made to "/web".
func MountWebBuild(mux goahttp.Muxer, h http.Handler) {
	mux.Handle("GET", "/web/", h.ServeHTTP)
	mux.Handle("GET", "/web/*filepath", h.ServeHTTP)
}