package models

import (
	"context"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

type Connection interface {
	QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
	Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error)
}

// We want to use a pool, connection, or transaction interchangeably throughout our model
// code. Verify they all satisfy a tiny common interface.
var _ Connection = &pgxpool.Pool{}
var _ Connection = &pgx.Conn{}
var _ Connection = pgx.Tx(nil)
