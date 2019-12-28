package pg2pubsub

import (
	"fmt"
	"time"

	"github.com/jackc/pgx/pgtype"
)

// Schema is a timestamped Avro schema object. We use the timestamp field to order schema
// updates, using the greatest timestamp value to represent the most recent schema update.
// The schema specification is a valid Avro schema.
type Schema struct {
	Timestamp time.Time           `json:"timestamp"`
	Spec      SchemaSpecification `json:"spec"`
}

type SchemaSpecification struct {
	Namespace string        `json:"namespace"`
	Type      string        `json:"type"`
	Name      string        `json:"name"`
	Fields    []SchemaField `json:"fields"`
}

func buildSchemaSpecification(relation *Relation) SchemaSpecification {
	spec := SchemaSpecification{
		Namespace: fmt.Sprintf("%s.%s", relation.Namespace, relation.Name),
		Name:      "value",
		Type:      "record",
		Fields:    []SchemaField{},
	}

	for _, column := range relation.Columns {
		spec.Fields = append(spec.Fields, marshalSchemaField(column))
	}

	return spec
}

type SchemaField struct {
	Name    string      `json:"name"`
	Type    []string    `json:"type"`
	Default interface{} `json:"default"`
}

// Avro provides a limited number of primitives that we need to map to Postgres OIDs. This
// SchemaField can perform this mapping, defaulting to string if not possible. All types
// should be nullable in order to allow deletions, given Avro's back/forward compatibility
// promise.
//
//   null: no value
//   boolean: a binary value
//   int: 32-bit signed integer
//   long: 64-bit signed integer
//   float: single precision (32-bit) IEEE 754 floating-point number
//   double: double precision (64-bit) IEEE 754 floating-point number
//   bytes: sequence of 8-bit unsigned bytes
//   string: unicode character sequence
//
func marshalSchemaField(c Column) SchemaField {
	var avroType string
	switch c.Type {
	case pgtype.BoolOID:
		avroType = "boolean"
	case pgtype.Int2OID, pgtype.Int4OID:
		avroType = "int"
	case pgtype.Int8OID:
		avroType = "long"
	case pgtype.Float4OID:
		avroType = "float"
	case pgtype.Float8OID:
		avroType = "double"
	default:
		avroType = "string"
	}

	return SchemaField{
		Name:    c.Name,
		Type:    []string{"null", avroType},
		Default: nil,
	}
}
