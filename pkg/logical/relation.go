package logical

import (
	"context"
	"fmt"

	"github.com/davecgh/go-spew/spew"
	"github.com/jackc/pgx/v4"
	"go.opencensus.io/trace"
)

// querier allows each helper to accept either transaction or connection objects
type querier interface {
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row
}

// BuildRelation generates a Relation structure by querying Postgres catalog tables.
// Importantly, this populates the relation.Columns slice, providing type information that
// can later be used to marshal Golang types.
func BuildRelation(ctx context.Context, tx querier, tableName, primaryKeyColumn string) (*Relation, error) {
	ctx, span := trace.StartSpan(ctx, "pkg/logical.BuildRelation")
	defer span.End()

	// Eg. oid = 16411, namespace = public, relname = example
	query := `
	select pg_class.oid as oid
	     , nspname as namespace
	     , relname as name
		from pg_class join pg_namespace on pg_class.relnamespace=pg_namespace.oid
	 where pg_class.oid = $1::regclass::oid;
	`

	relation := &Relation{Columns: []Column{}}
	err := tx.QueryRow(ctx, query, tableName).Scan(&relation.ID, &relation.Namespace, &relation.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to identify table namespace and name: %w", err)
	}

	// Eg. name = id, type = 20
	columnQuery := `
	select attname as name
			 , atttypid as type
	  from pg_attribute
	 where attrelid = $1 and attnum > 0 and not attisdropped
	 order by attnum;
	`

	rows, err := tx.Query(ctx, columnQuery, relation.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_attribute for relation columns: %w", err)
	}

	defer rows.Close()

	for rows.Next() {
		column := Column{}
		if err := rows.Scan(&column.Name, &column.Type); err != nil {
			return nil, err
		}

		// We don't strictly require this, but it's asking for trouble to generate
		// logical.Relation structs that have incorrect key metadata.
		if column.Name == primaryKeyColumn {
			column.Key = true
		}

		relation.Columns = append(relation.Columns, column)
	}

	return relation, nil
}

// Relation would normally include a column count field, but given Go slices track their
// size it becomes unnecessary.
//
// Unlike the other message types, the relation type is special, as there is a lot of
// functionality around its construction and use, primarily because the relation type
// allows us to interpret raw Postgres tuples into Golang native types.
type Relation struct {
	ID              uint32   // ID of the relation.
	Namespace       string   // Namespace (empty string for pg_catalog).
	Name            string   // Relation name.
	ReplicaIdentity uint8    // Replica identity setting for the relation (same as relreplident in pg_class).
	Columns         []Column // Repeating message of column definitions.
}

// String provides the fully-qualified <schema>.<table> identifer for the relation
func (r Relation) String() string {
	return fmt.Sprintf("%s.%s", r.Namespace, r.Name)
}

// Marshal converts a tuple into a dynamic Golang map type. Values are represented in Go
// native types.
func (r *Relation) Marshal(tuple []Element) map[string]interface{} {
	// This tuple doesn't match our relation, if the sizes aren't the same
	if len(tuple) != len(r.Columns) {
		return nil
	}

	row := map[string]interface{}{}
	for idx, column := range r.Columns {
		var decoded interface{}
		if tuple[idx].Value != nil {
			var err error
			decoded, err = column.Decode(tuple[idx].Value)
			if err != nil {
				panic(fmt.Sprintf("failed to decode tuple value: %v\n\n%s", err, spew.Sdump(err)))
			}
		}

		row[column.Name] = decoded
	}

	return row
}
