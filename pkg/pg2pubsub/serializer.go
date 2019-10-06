package pg2pubsub

import (
	"fmt"
	"time"

	kitlog "github.com/go-kit/kit/log"
	"github.com/jackc/pgx/pgtype"
)

func Serialize(logger kitlog.Logger, sub *Subscription) (<-chan TimestampedSchema, <-chan Modification) {
	registry := registry{}
	schemas, modifications := make(chan TimestampedSchema), make(chan Modification)

	go func() {
		for committed := range DecorateCommits(sub.Received()) {
			switch entry := committed.Entry.(type) {
			case *Relation:
				logger.Log("event", "adding_relation", "id", entry.ID, "name", entry.Name)
				registry.Register(entry)

				schemas <- marshalTimestampedSchema(committed)
			case *Insert, *Update, *Delete:
				modifications <- registry.Serialize(committed)
			}
		}
	}()

	return schemas, modifications
}

type registry map[uint32]*Relation

func (r registry) Register(relation *Relation) {
	r[relation.ID] = relation
}

func (r registry) Serialize(committed Committed) Modification {
	modification := Modification{
		Timestamp: committed.Timestamp,
		LSN:       committed.LSN,
		Sequence:  committed.Sequence,
	}

	var relation *Relation

	switch cast := committed.Entry.(type) {
	case *Insert:
		relation = r[cast.ID]
		modification.Before = relation.Marshal(cast.Row)
	case *Update:
		relation = r[cast.ID]
		modification.Before, modification.After = relation.Marshal(cast.OldRow), relation.Marshal(cast.Row)
	case *Delete:
		relation = r[cast.ID]
		modification.Before = relation.Marshal(cast.OldRow)
	default:
		panic("invalid message type")
	}

	modification.Namespace = relation.Namespace
	modification.Name = relation.Name

	return modification
}

type Modification struct {
	Timestamp time.Time         `json:"timestamp"`
	LSN       uint64            `json:"lsn"`
	Sequence  uint64            `json:"sequence"`
	Namespace string            `json:"namespace"`
	Name      string            `json:"name"`
	Before    map[string]string `json:"before"`
	After     map[string]string `json:"after"`
}

type TimestampedSchema struct {
	Timestamp time.Time `json:"timestamp"`
	Schema    Schema    `json:"schema"`
}

type Schema struct {
	Namespace string        `json:"namespace"`
	Type      string        `json:"type"`
	Name      string        `json:"name"`
	Fields    []SchemaField `json:"fields"`
}

func marshalTimestampedSchema(committed Committed) TimestampedSchema {
	relation := committed.Entry.(*Relation)
	schema := Schema{
		Namespace: fmt.Sprintf("%s.%s", relation.Namespace, relation.Name),
		Name:      "value",
		Type:      "record",
		Fields:    []SchemaField{},
	}

	for _, column := range relation.Columns {
		schema.Fields = append(schema.Fields, marshalSchemaField(column))
	}

	return TimestampedSchema{
		Timestamp: committed.Timestamp,
		Schema:    schema,
	}
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
/*
null: no value
boolean: a binary value
int: 32-bit signed integer
long: 64-bit signed integer
float: single precision (32-bit) IEEE 754 floating-point number
double: double precision (64-bit) IEEE 754 floating-point number
bytes: sequence of 8-bit unsigned bytes
string: unicode character sequence
*/
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

// Committed wraps a logical decoded message with the begin message associated with the
// transaction that the message was contained within, along with a sequence number that
// can be used to order operations within the same transaction.
type Committed struct {
	Begin
	Sequence uint64
	Entry    interface{}
}

// DecorateCommits receives a channel containing logical replication messages and produces
// a channel which annotates each message with commit information. Committed structs can
// be tracked back to a specific LSN, and logically ordered by sequence number, ensuring
// we can detect the authoriative row value even if the same row is updated many times
// within the same transaction.
func DecorateCommits(operations <-chan interface{}) chan Committed {
	output := make(chan Committed)

	go func() {
		var currentTransaction *Begin
		var sequence uint64

		for msg := range operations {
			switch msg := msg.(type) {
			case *Begin:
				currentTransaction = msg
				sequence = 0
			case *Commit:
				currentTransaction = nil
			default:
				sequence++
				output <- Committed{
					Begin:    *currentTransaction,
					Sequence: sequence,
					Entry:    msg,
				}
			}
		}

		close(output)
	}()

	return output
}
