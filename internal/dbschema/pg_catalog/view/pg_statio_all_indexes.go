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

var PgStatioAllIndexes = newPgStatioAllIndexesTable()

type pgStatioAllIndexesTable struct {
	postgres.Table

	//Columns
	Relid        postgres.ColumnString
	Indexrelid   postgres.ColumnString
	Schemaname   postgres.ColumnString
	Relname      postgres.ColumnString
	Indexrelname postgres.ColumnString
	IdxBlksRead  postgres.ColumnInteger
	IdxBlksHit   postgres.ColumnInteger

	AllColumns     postgres.ColumnList
	MutableColumns postgres.ColumnList
}

type PgStatioAllIndexesTable struct {
	pgStatioAllIndexesTable

	EXCLUDED pgStatioAllIndexesTable
}

// AS creates new PgStatioAllIndexesTable with assigned alias
func (a *PgStatioAllIndexesTable) AS(alias string) *PgStatioAllIndexesTable {
	aliasTable := newPgStatioAllIndexesTable()
	aliasTable.Table.AS(alias)
	return aliasTable
}

func newPgStatioAllIndexesTable() *PgStatioAllIndexesTable {
	return &PgStatioAllIndexesTable{
		pgStatioAllIndexesTable: newPgStatioAllIndexesTableImpl("pg_catalog", "pg_statio_all_indexes"),
		EXCLUDED:                newPgStatioAllIndexesTableImpl("", "excluded"),
	}
}

func newPgStatioAllIndexesTableImpl(schemaName, tableName string) pgStatioAllIndexesTable {
	var (
		RelidColumn        = postgres.StringColumn("relid")
		IndexrelidColumn   = postgres.StringColumn("indexrelid")
		SchemanameColumn   = postgres.StringColumn("schemaname")
		RelnameColumn      = postgres.StringColumn("relname")
		IndexrelnameColumn = postgres.StringColumn("indexrelname")
		IdxBlksReadColumn  = postgres.IntegerColumn("idx_blks_read")
		IdxBlksHitColumn   = postgres.IntegerColumn("idx_blks_hit")
		allColumns         = postgres.ColumnList{RelidColumn, IndexrelidColumn, SchemanameColumn, RelnameColumn, IndexrelnameColumn, IdxBlksReadColumn, IdxBlksHitColumn}
		mutableColumns     = postgres.ColumnList{RelidColumn, IndexrelidColumn, SchemanameColumn, RelnameColumn, IndexrelnameColumn, IdxBlksReadColumn, IdxBlksHitColumn}
	)

	return pgStatioAllIndexesTable{
		Table: postgres.NewTable(schemaName, tableName, allColumns...),

		//Columns
		Relid:        RelidColumn,
		Indexrelid:   IndexrelidColumn,
		Schemaname:   SchemanameColumn,
		Relname:      RelnameColumn,
		Indexrelname: IndexrelnameColumn,
		IdxBlksRead:  IdxBlksReadColumn,
		IdxBlksHit:   IdxBlksHitColumn,

		AllColumns:     allColumns,
		MutableColumns: mutableColumns,
	}
}
