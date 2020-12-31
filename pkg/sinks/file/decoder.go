package file

import (
	"time"

	"github.com/jackc/pgtype"
	"github.com/lawrencejones/pgsink/pkg/types"
)

// Decoder is the most vanila of decoders, given the file sink supports the most basic of
// types, and we don't use the externalType field.
var Decoder = types.DecoderFunc(func(oid uint32) (scanner types.ValueScanner, empty interface{}, externalType interface{}) {
	switch oid {
	case pgtype.BoolOID:
		return &pgtype.Bool{}, true, nil
	case pgtype.Int2OID:
		return &pgtype.Int2{}, int16(0), nil
	case pgtype.Int4OID:
		return &pgtype.Int4{}, int32(0), nil
	case pgtype.Int8OID:
		return &pgtype.Int8{}, int64(0), nil
	case pgtype.Float4OID:
		return &pgtype.Float4{}, float32(0.0), nil
	case pgtype.Float8OID:
		return &pgtype.Float8{}, float64(0.0), nil
	case pgtype.TimestampOID:
		return &pgtype.Timestamp{}, time.Time{}, nil
	case pgtype.TimestamptzOID:
		return &pgtype.Timestamptz{}, time.Time{}, nil
	}

	return &pgtype.Text{}, "", nil
})
