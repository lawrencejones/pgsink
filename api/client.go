package api

import (
	"net/url"

	goahttp "goa.design/goa/v3/http"

	health "github.com/lawrencejones/pgsink/api/gen/health"
	healthhttp "github.com/lawrencejones/pgsink/api/gen/http/health/client"
	imports "github.com/lawrencejones/pgsink/api/gen/imports"
	importshttp "github.com/lawrencejones/pgsink/api/gen/http/imports/client"
	subscriptions "github.com/lawrencejones/pgsink/api/gen/subscriptions"
	subscriptionshttp "github.com/lawrencejones/pgsink/api/gen/http/subscriptions/client"
	tables "github.com/lawrencejones/pgsink/api/gen/tables"
	tableshttp "github.com/lawrencejones/pgsink/api/gen/http/tables/client"
)

type Client struct {
	Health *health.Client
	Imports *imports.Client
	Subscriptions *subscriptions.Client
	Tables *tables.Client
}

// NewHTTPClient provides a client that combines all services together. It's not great, as
// you have to manually adjust things whenever you regenerate the binary- there must be a
// better way, though I haven't looked too hard just yet.
func NewHTTPClient(endpoint string, doer goahttp.Doer) (*Client, error) {
	uri, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}

	client := new(Client)
	{
		http := healthhttp.NewClient(
			uri.Scheme, uri.Host, doer, goahttp.RequestEncoder, goahttp.ResponseDecoder, false)
		client.Health = health.NewClient(http.Check())
	}
	{
		http := importshttp.NewClient(
			uri.Scheme, uri.Host, doer, goahttp.RequestEncoder, goahttp.ResponseDecoder, false)
		client.Imports = imports.NewClient(http.List())
	}
	{
		http := subscriptionshttp.NewClient(
			uri.Scheme, uri.Host, doer, goahttp.RequestEncoder, goahttp.ResponseDecoder, false)
		client.Subscriptions = subscriptions.NewClient(http.Get(), http.AddTable(), http.StopTable())
	}
	{
		http := tableshttp.NewClient(
			uri.Scheme, uri.Host, doer, goahttp.RequestEncoder, goahttp.ResponseDecoder, false)
		client.Tables = tables.NewClient(http.List())
	}

	return client, nil
}
