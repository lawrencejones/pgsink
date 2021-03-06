//
// Code generated by go-jet DO NOT EDIT.
//
// WARNING: Changes to this file may cause incorrect behavior
// and will be lost if the code is regenerated
//

package view

import (
	"github.com/go-jet/jet/v2/postgres"
)

var PgMatviews = newPgMatviewsTable()

type pgMatviewsTable struct {
	postgres.Table

	//Columns
	Schemaname   postgres.ColumnString
	Matviewname  postgres.ColumnString
	Matviewowner postgres.ColumnString
	Tablespace   postgres.ColumnString
	Hasindexes   postgres.ColumnBool
	Ispopulated  postgres.ColumnBool
	Definition   postgres.ColumnString

	AllColumns     postgres.ColumnList
	MutableColumns postgres.ColumnList
}

type PgMatviewsTable struct {
	pgMatviewsTable

	EXCLUDED pgMatviewsTable
}

// AS creates new PgMatviewsTable with assigned alias
func (a *PgMatviewsTable) AS(alias string) *PgMatviewsTable {
	aliasTable := newPgMatviewsTable()
	aliasTable.Table.AS(alias)
	return aliasTable
}

func newPgMatviewsTable() *PgMatviewsTable {
	return &PgMatviewsTable{
		pgMatviewsTable: newPgMatviewsTableImpl("pg_catalog", "pg_matviews"),
		EXCLUDED:        newPgMatviewsTableImpl("", "excluded"),
	}
}

func newPgMatviewsTableImpl(schemaName, tableName string) pgMatviewsTable {
	var (
		SchemanameColumn   = postgres.StringColumn("schemaname")
		MatviewnameColumn  = postgres.StringColumn("matviewname")
		MatviewownerColumn = postgres.StringColumn("matviewowner")
		TablespaceColumn   = postgres.StringColumn("tablespace")
		HasindexesColumn   = postgres.BoolColumn("hasindexes")
		IspopulatedColumn  = postgres.BoolColumn("ispopulated")
		DefinitionColumn   = postgres.StringColumn("definition")
		allColumns         = postgres.ColumnList{SchemanameColumn, MatviewnameColumn, MatviewownerColumn, TablespaceColumn, HasindexesColumn, IspopulatedColumn, DefinitionColumn}
		mutableColumns     = postgres.ColumnList{SchemanameColumn, MatviewnameColumn, MatviewownerColumn, TablespaceColumn, HasindexesColumn, IspopulatedColumn, DefinitionColumn}
	)

	return pgMatviewsTable{
		Table: postgres.NewTable(schemaName, tableName, allColumns...),

		//Columns
		Schemaname:   SchemanameColumn,
		Matviewname:  MatviewnameColumn,
		Matviewowner: MatviewownerColumn,
		Tablespace:   TablespaceColumn,
		Hasindexes:   HasindexesColumn,
		Ispopulated:  IspopulatedColumn,
		Definition:   DefinitionColumn,

		AllColumns:     allColumns,
		MutableColumns: mutableColumns,
	}
}
