// Code generated by goa v3.5.4, DO NOT EDIT.
//
// Subscriptions client HTTP transport
//
// Command:
// $ goa gen github.com/lawrencejones/pgsink/api/design -o api

package client

import (
	"context"
	"net/http"

	goahttp "goa.design/goa/v3/http"
	goa "goa.design/goa/v3/pkg"
)

// Client lists the Subscriptions service endpoint HTTP clients.
type Client struct {
	// Get Doer is the HTTP client used to make requests to the Get endpoint.
	GetDoer goahttp.Doer

	// AddTable Doer is the HTTP client used to make requests to the AddTable
	// endpoint.
	AddTableDoer goahttp.Doer

	// StopTable Doer is the HTTP client used to make requests to the StopTable
	// endpoint.
	StopTableDoer goahttp.Doer

	// RestoreResponseBody controls whether the response bodies are reset after
	// decoding so they can be read again.
	RestoreResponseBody bool

	scheme  string
	host    string
	encoder func(*http.Request) goahttp.Encoder
	decoder func(*http.Response) goahttp.Decoder
}

// NewClient instantiates HTTP clients for all the Subscriptions service
// servers.
func NewClient(
	scheme string,
	host string,
	doer goahttp.Doer,
	enc func(*http.Request) goahttp.Encoder,
	dec func(*http.Response) goahttp.Decoder,
	restoreBody bool,
) *Client {
	return &Client{
		GetDoer:             doer,
		AddTableDoer:        doer,
		StopTableDoer:       doer,
		RestoreResponseBody: restoreBody,
		scheme:              scheme,
		host:                host,
		decoder:             dec,
		encoder:             enc,
	}
}

// Get returns an endpoint that makes HTTP requests to the Subscriptions
// service Get server.
func (c *Client) Get() goa.Endpoint {
	var (
		decodeResponse = DecodeGetResponse(c.decoder, c.RestoreResponseBody)
	)
	return func(ctx context.Context, v interface{}) (interface{}, error) {
		req, err := c.BuildGetRequest(ctx, v)
		if err != nil {
			return nil, err
		}
		resp, err := c.GetDoer.Do(req)
		if err != nil {
			return nil, goahttp.ErrRequestError("Subscriptions", "Get", err)
		}
		return decodeResponse(resp)
	}
}

// AddTable returns an endpoint that makes HTTP requests to the Subscriptions
// service AddTable server.
func (c *Client) AddTable() goa.Endpoint {
	var (
		encodeRequest  = EncodeAddTableRequest(c.encoder)
		decodeResponse = DecodeAddTableResponse(c.decoder, c.RestoreResponseBody)
	)
	return func(ctx context.Context, v interface{}) (interface{}, error) {
		req, err := c.BuildAddTableRequest(ctx, v)
		if err != nil {
			return nil, err
		}
		err = encodeRequest(req, v)
		if err != nil {
			return nil, err
		}
		resp, err := c.AddTableDoer.Do(req)
		if err != nil {
			return nil, goahttp.ErrRequestError("Subscriptions", "AddTable", err)
		}
		return decodeResponse(resp)
	}
}

// StopTable returns an endpoint that makes HTTP requests to the Subscriptions
// service StopTable server.
func (c *Client) StopTable() goa.Endpoint {
	var (
		encodeRequest  = EncodeStopTableRequest(c.encoder)
		decodeResponse = DecodeStopTableResponse(c.decoder, c.RestoreResponseBody)
	)
	return func(ctx context.Context, v interface{}) (interface{}, error) {
		req, err := c.BuildStopTableRequest(ctx, v)
		if err != nil {
			return nil, err
		}
		err = encodeRequest(req, v)
		if err != nil {
			return nil, err
		}
		resp, err := c.StopTableDoer.Do(req)
		if err != nil {
			return nil, goahttp.ErrRequestError("Subscriptions", "StopTable", err)
		}
		return decodeResponse(resp)
	}
}
