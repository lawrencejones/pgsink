//
// Code generated by go-jet DO NOT EDIT.
// Generated at Tuesday, 12-May-20 07:59:32 BST
//
// WARNING: Changes to this file may cause incorrect behavior
// and will be lost if the code is regenerated
//

package table

import (
	"github.com/go-jet/jet/postgres"
)

var PgLargeobjectMetadata = newPgLargeobjectMetadataTable()

type PgLargeobjectMetadataTable struct {
	postgres.Table

	//Columns
	Oid      postgres.ColumnString
	Lomowner postgres.ColumnString
	Lomacl   postgres.ColumnString

	AllColumns     postgres.ColumnList
	MutableColumns postgres.ColumnList
}

// creates new PgLargeobjectMetadataTable with assigned alias
func (a *PgLargeobjectMetadataTable) AS(alias string) *PgLargeobjectMetadataTable {
	aliasTable := newPgLargeobjectMetadataTable()

	aliasTable.Table.AS(alias)

	return aliasTable
}

func newPgLargeobjectMetadataTable() *PgLargeobjectMetadataTable {
	var (
		OidColumn      = postgres.StringColumn("oid")
		LomownerColumn = postgres.StringColumn("lomowner")
		LomaclColumn   = postgres.StringColumn("lomacl")
	)

	return &PgLargeobjectMetadataTable{
		Table: postgres.NewTable("pg_catalog", "pg_largeobject_metadata", OidColumn, LomownerColumn, LomaclColumn),

		//Columns
		Oid:      OidColumn,
		Lomowner: LomownerColumn,
		Lomacl:   LomaclColumn,

		AllColumns:     postgres.ColumnList{OidColumn, LomownerColumn, LomaclColumn},
		MutableColumns: postgres.ColumnList{OidColumn, LomownerColumn, LomaclColumn},
	}
}