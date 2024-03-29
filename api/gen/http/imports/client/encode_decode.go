// Code generated by goa v3.5.4, DO NOT EDIT.
//
// Imports HTTP client encoders and decoders
//
// Command:
// $ goa gen github.com/lawrencejones/pgsink/api/design -o api

package client

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"net/url"

	imports "github.com/lawrencejones/pgsink/api/gen/imports"
	goahttp "goa.design/goa/v3/http"
	goa "goa.design/goa/v3/pkg"
)

// BuildListRequest instantiates a HTTP request object with method and path set
// to call the "Imports" service "List" endpoint
func (c *Client) BuildListRequest(ctx context.Context, v interface{}) (*http.Request, error) {
	u := &url.URL{Scheme: c.scheme, Host: c.host, Path: ListImportsPath()}
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, goahttp.ErrInvalidURL("Imports", "List", u.String(), err)
	}
	if ctx != nil {
		req = req.WithContext(ctx)
	}

	return req, nil
}

// DecodeListResponse returns a decoder for responses returned by the Imports
// List endpoint. restoreBody controls whether the response body should be
// restored after having been read.
func DecodeListResponse(decoder func(*http.Response) goahttp.Decoder, restoreBody bool) func(*http.Response) (interface{}, error) {
	return func(resp *http.Response) (interface{}, error) {
		if restoreBody {
			b, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				return nil, err
			}
			resp.Body = ioutil.NopCloser(bytes.NewBuffer(b))
			defer func() {
				resp.Body = ioutil.NopCloser(bytes.NewBuffer(b))
			}()
		} else {
			defer resp.Body.Close()
		}
		switch resp.StatusCode {
		case http.StatusOK:
			var (
				body ListResponseBody
				err  error
			)
			err = decoder(resp).Decode(&body)
			if err != nil {
				return nil, goahttp.ErrDecodingError("Imports", "List", err)
			}
			for _, e := range body {
				if e != nil {
					if err2 := ValidateImportResponse(e); err2 != nil {
						err = goa.MergeErrors(err, err2)
					}
				}
			}
			if err != nil {
				return nil, goahttp.ErrValidationError("Imports", "List", err)
			}
			res := NewListImportOK(body)
			return res, nil
		default:
			body, _ := ioutil.ReadAll(resp.Body)
			return nil, goahttp.ErrInvalidResponse("Imports", "List", resp.StatusCode, string(body))
		}
	}
}

// unmarshalImportResponseToImportsImport builds a value of type
// *imports.Import from a value of type *ImportResponse.
func unmarshalImportResponseToImportsImport(v *ImportResponse) *imports.Import {
	res := &imports.Import{
		ID:                 *v.ID,
		SubscriptionID:     *v.SubscriptionID,
		Schema:             *v.Schema,
		TableName:          *v.TableName,
		CompletedAt:        v.CompletedAt,
		CreatedAt:          *v.CreatedAt,
		UpdatedAt:          *v.UpdatedAt,
		ExpiredAt:          v.ExpiredAt,
		Error:              v.Error,
		ErrorCount:         *v.ErrorCount,
		LastErrorAt:        v.LastErrorAt,
		RowsProcessedTotal: *v.RowsProcessedTotal,
	}

	return res
}
