// Code generated by goa v3.5.4, DO NOT EDIT.
//
// Subscriptions endpoints
//
// Command:
// $ goa gen github.com/lawrencejones/pgsink/api/design -o api

package subscriptions

import (
	"context"

	goa "goa.design/goa/v3/pkg"
)

// Endpoints wraps the "Subscriptions" service endpoints.
type Endpoints struct {
	Get       goa.Endpoint
	AddTable  goa.Endpoint
	StopTable goa.Endpoint
}

// NewEndpoints wraps the methods of the "Subscriptions" service with endpoints.
func NewEndpoints(s Service) *Endpoints {
	return &Endpoints{
		Get:       NewGetEndpoint(s),
		AddTable:  NewAddTableEndpoint(s),
		StopTable: NewStopTableEndpoint(s),
	}
}

// Use applies the given middleware to all the "Subscriptions" service
// endpoints.
func (e *Endpoints) Use(m func(goa.Endpoint) goa.Endpoint) {
	e.Get = m(e.Get)
	e.AddTable = m(e.AddTable)
	e.StopTable = m(e.StopTable)
}

// NewGetEndpoint returns an endpoint function that calls the method "Get" of
// service "Subscriptions".
func NewGetEndpoint(s Service) goa.Endpoint {
	return func(ctx context.Context, req interface{}) (interface{}, error) {
		return s.Get(ctx)
	}
}

// NewAddTableEndpoint returns an endpoint function that calls the method
// "AddTable" of service "Subscriptions".
func NewAddTableEndpoint(s Service) goa.Endpoint {
	return func(ctx context.Context, req interface{}) (interface{}, error) {
		p := req.(*SubscriptionPublishedTable)
		return s.AddTable(ctx, p)
	}
}

// NewStopTableEndpoint returns an endpoint function that calls the method
// "StopTable" of service "Subscriptions".
func NewStopTableEndpoint(s Service) goa.Endpoint {
	return func(ctx context.Context, req interface{}) (interface{}, error) {
		p := req.(*SubscriptionPublishedTable)
		return s.StopTable(ctx, p)
	}
}
