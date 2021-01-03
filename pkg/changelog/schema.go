package changelog

import (
	"crypto/md5"
	"fmt"
	"time"

	"github.com/lawrencejones/pgsink/pkg/logical"
)

// Schema defines the structure of data pulled from Postgres. It can be generated from an
// existing logical.Relation, when combined with a decoder that translates the Postgres
// types to Golang.
//
// In future, we'll want to be able to translate this schema type into official formats,
// like Avro.
type Schema struct {
	Timestamp time.Time           `json:"timestamp"` // commit timestamp
	Namespace string              `json:"namespace"` // Postgres schema
	Name      string              `json:"name"`      // Postgres table name
	LSN       *uint64             `json:"lsn"`       // log sequence number, where appropriate
	Spec      SchemaSpecification `json:"spec"`      // schema definition
}

func (s Schema) String() string {
	return fmt.Sprintf("%s.%s", s.Namespace, s.Name)
}

type SchemaSpecification struct {
	Columns []logical.Column `json:"columns"` // Postgres columns
}

// SchemaFromRelation uses a logical.Relation and decoder to generate an intermediate schema
func SchemaFromRelation(timestamp time.Time, lsn *uint64, relation *logical.Relation) Schema {
	return Schema{
		Timestamp: timestamp,
		Namespace: relation.Namespace,
		Name:      relation.Name,
		LSN:       lsn,
		Spec: SchemaSpecification{
			Columns: relation.Columns,
		},
	}
}

// GetFingerprint returns a unique idenfier for the schema.
//
// The only important thing is that any given schema returns the same fingerprint for the
// duration of the Go process. Beyond that, you can use any value here.
func (s Schema) GetFingerprint() string {
	h := md5.New()
	for _, column := range s.Spec.Columns {
		fmt.Fprintf(h, "%v|%v|%v|%v\n", column.Key, column.Name, column.Type, column.Modifier)
	}
	return string(h.Sum(nil))
}
