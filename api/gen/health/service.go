// Code generated by goa v3.5.4, DO NOT EDIT.
//
// Health service
//
// Command:
// $ goa gen github.com/lawrencejones/pgsink/api/design -o api

package health

import (
	"context"
)

// Provide service health information
type Service interface {
	// Health check for probes
	Check(context.Context) (res *CheckResult, err error)
}

// ServiceName is the name of the service as defined in the design. This is the
// same value that is set in the endpoint request contexts under the ServiceKey
// key.
const ServiceName = "Health"

// MethodNames lists the service method names as defined in the design. These
// are the same values that are set in the endpoint request contexts under the
// MethodKey key.
var MethodNames = [1]string{"Check"}

// CheckResult is the result type of the Health service Check method.
type CheckResult struct {
	// Status of the API
	Status string
}
