package changelog

import (
	"encoding/json"
	"time"

	"github.com/jackc/pgx/pgtype"
	"github.com/lawrencejones/pg2sink/pkg/logical"
	goavro "github.com/linkedin/goavro/v2"
)

// Schema is a timestamped Avro schema object. We use the timestamp field to order schema
// updates, using the greatest timestamp value to represent the most recent schema update.
// The schema specification is a valid Avro schema.
type Schema struct {
	Timestamp time.Time           `json:"timestamp"` // commit timestamp
	LSN       *uint64             `json:"lsn"`       // log sequence number, where appropriate
	Spec      SchemaSpecification `json:"spec"`      // Avro schema format
}

// SchemaFromRelation generates a schema from a logical relation message
func SchemaFromRelation(timestamp time.Time, lsn *uint64, relation *logical.Relation) Schema {
	spec := SchemaSpecification{
		Namespace: BuildNamespace(relation.Namespace, relation.Name),
		Type:      schemaType,
		Name:      schemaName,
		Fields:    []SchemaField{},
	}

	for _, column := range relation.Columns {
		spec.Fields = append(spec.Fields, schemaFieldFromColumn(column))
	}

	return Schema{
		Timestamp: timestamp,
		LSN:       lsn,
		Spec:      spec,
	}
}

// Be consistent when structuring Avro schemas. The table schema & namespace denotes the
// Avro namespace, while the type of the schema is always record and the name always
// value.
const (
	schemaType = "record"
	schemaName = "value"
)

// SchemaSpecification is an Avro compliant schema format. We store all schemas for a
// Postgres table under the same namespace, and a row inside this table has a schema of
// type 'record' named 'value'.
type SchemaSpecification struct {
	Namespace Namespace     `json:"namespace"` // <schema>.<table>
	Type      string        `json:"type"`      // always record
	Name      string        `json:"name"`      // always value
	Fields    []SchemaField `json:"fields"`    // schema fields
}

// GetFingerprint returns a unique idenfier for the schema. It currently uses the goavro
// Rabin calculation to produce a fingerprint, but we should move to something less janky
// if it becomes a performance problem.
//
// The only important thing is that any given schema returns the same fingerprint for the
// duration of the Go process. Beyond that, you can use any value here.
func (s SchemaSpecification) GetFingerprint() uint64 {
	jsonBytes, err := json.Marshal(s)
	if err != nil {
		panic("failed to marshal SchemaSpecification into json")
	}

	codec, err := goavro.NewCodec(string(jsonBytes))
	if err != nil {
		panic("failed to parse Avro codec from SchemaSpecification")
	}

	return codec.Rabin
}

type SchemaField struct {
	Name    string      `json:"name"`
	Type    []string    `json:"type"`
	Default interface{} `json:"default"`
}

func (s SchemaField) GetType() string {
	for _, t := range s.Type {
		if t != "null" {
			return t
		}
	}

	panic("all schema fields should have a non-null type")
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
func schemaFieldFromColumn(c logical.Column) SchemaField {
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
