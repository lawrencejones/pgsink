// Code generated by goa v3.2.6, DO NOT EDIT.
//
// Imports endpoints
//
// Command:
// $ goa gen github.com/lawrencejones/pgsink/api/design -o api

package imports

import (
	"context"

	goa "goa.design/goa/v3/pkg"
)

// Endpoints wraps the "Imports" service endpoints.
type Endpoints struct {
	List goa.Endpoint
}

// NewEndpoints wraps the methods of the "Imports" service with endpoints.
func NewEndpoints(s Service) *Endpoints {
	return &Endpoints{
		List: NewListEndpoint(s),
	}
}

// Use applies the given middleware to all the "Imports" service endpoints.
func (e *Endpoints) Use(m func(goa.Endpoint) goa.Endpoint) {
	e.List = m(e.List)
}

// NewListEndpoint returns an endpoint function that calls the method "List" of
// service "Imports".
func NewListEndpoint(s Service) goa.Endpoint {
	return func(ctx context.Context, req interface{}) (interface{}, error) {
		return s.List(ctx)
	}
}