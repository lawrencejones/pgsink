package file

import (
	"time"

	"github.com/jackc/pgtype"
	"github.com/lawrencejones/pgsink/pkg/decode"
)

// Decoder is the most vanila of decoders, given the file sink supports the most basic of
// types, and we don't use the externalType field.
var Decoder = decode.Build(
	decode.Build.WithType(pgtype.BoolOID, &pgtype.Bool{}, true, nil),
	decode.Build.WithType(pgtype.Int2OID, &pgtype.Int2{}, int16(0), nil),
	decode.Build.WithType(pgtype.Int4OID, &pgtype.Int4{}, int32(0), nil),
	decode.Build.WithType(pgtype.Int8OID, &pgtype.Int8{}, int64(0), nil),
	decode.Build.WithType(pgtype.Float4OID, &pgtype.Float4{}, float32(0.0), nil),
	decode.Build.WithType(pgtype.Float8OID, &pgtype.Float8{}, float64(0.0), nil),
	decode.Build.WithType(pgtype.TimestampOID, &pgtype.Timestamp{}, time.Time{}, nil),
	decode.Build.WithType(pgtype.TimestamptzOID, &pgtype.Timestamptz{}, time.Time{}, nil),
)
