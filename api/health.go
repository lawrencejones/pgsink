package api

import (
	"context"

	"github.com/lawrencejones/pgsink/api/gen/health"
)

type healthService struct {
}

func NewHealth() health.Service {
	return &healthService{}
}

func (h *healthService) Check(ctx context.Context) (res *health.CheckResult, err error) {
	return &health.CheckResult{Status: "healthy"}, nil
}
