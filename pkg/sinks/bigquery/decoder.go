package bigquery

import (
	"fmt"
	"reflect"
	"time"

	bq "cloud.google.com/go/bigquery"
)

// fieldTypeFor maps Postgres OID types to BigQuery types, allowing us to build BigQuery
// schemas from Postgres type information.
func fieldTypeFor(empty interface{}) (fieldType bq.FieldType, repeated bool, err error) {
	// We receive an empty paramter, generated from a mapping.NewEmpty, or the destination
	// of a scanner. We want to unpack that to get the typed pointer, as the empty value
	// will be double pointed.
	val := reflect.ValueOf(empty).Elem().Interface()

	switch val.(type) {
	case *bool:
		return bq.BooleanFieldType, repeated, nil
	case *int8, *int16, *int32, *int64:
		return bq.IntegerFieldType, repeated, nil
	case *uint8, *uint16, *uint32, *uint64:
		return bq.IntegerFieldType, repeated, nil
	case *float32, *float64:
		return bq.FloatFieldType, repeated, nil
	case *time.Time:
		return bq.TimestampFieldType, repeated, nil
	case *string:
		return bq.StringFieldType, repeated, nil
	}

	// All types that follow must be repeated
	repeated = true

	// Composite types
	switch val.(type) {
	case *[]bool:
		return bq.BooleanFieldType, repeated, nil
	case *[]int8, *[]int16, *[]int32, *[]int64:
		return bq.IntegerFieldType, repeated, nil
	case *[]uint8, *[]uint16, *[]uint32, *[]uint64:
		return bq.IntegerFieldType, repeated, nil
	case *[]float32, *[]float64:
		return bq.FloatFieldType, repeated, nil
	case *[]time.Time:
		return bq.TimestampFieldType, repeated, nil
	case *[]string:
		return bq.StringFieldType, repeated, nil
	}

	return "", false, fmt.Errorf("no BigQuery field for type %T", val)
}
