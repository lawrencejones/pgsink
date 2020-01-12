package models

import (
	"context"

	"github.com/jackc/pgx"
)

type Connection interface {
	QueryRowEx(ctx context.Context, sql string, options *pgx.QueryExOptions, args ...interface{}) *pgx.Row
	QueryEx(ctx context.Context, sql string, options *pgx.QueryExOptions, args ...interface{}) (*pgx.Rows, error)
	ExecEx(ctx context.Context, sql string, options *pgx.QueryExOptions, args ...interface{}) (pgx.CommandTag, error)
}

// We want all these connection constructs to satisfy Connection
var _ Connection = &pgx.ConnPool{}
var _ Connection = &pgx.Conn{}
var _ Connection = &pgx.Tx{}
