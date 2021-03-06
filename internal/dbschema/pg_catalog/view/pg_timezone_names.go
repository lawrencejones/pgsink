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

var PgTimezoneNames = newPgTimezoneNamesTable()

type pgTimezoneNamesTable struct {
	postgres.Table

	//Columns
	Name      postgres.ColumnString
	Abbrev    postgres.ColumnString
	UtcOffset postgres.ColumnInterval
	IsDst     postgres.ColumnBool

	AllColumns     postgres.ColumnList
	MutableColumns postgres.ColumnList
}

type PgTimezoneNamesTable struct {
	pgTimezoneNamesTable

	EXCLUDED pgTimezoneNamesTable
}

// AS creates new PgTimezoneNamesTable with assigned alias
func (a *PgTimezoneNamesTable) AS(alias string) *PgTimezoneNamesTable {
	aliasTable := newPgTimezoneNamesTable()
	aliasTable.Table.AS(alias)
	return aliasTable
}

func newPgTimezoneNamesTable() *PgTimezoneNamesTable {
	return &PgTimezoneNamesTable{
		pgTimezoneNamesTable: newPgTimezoneNamesTableImpl("pg_catalog", "pg_timezone_names"),
		EXCLUDED:             newPgTimezoneNamesTableImpl("", "excluded"),
	}
}

func newPgTimezoneNamesTableImpl(schemaName, tableName string) pgTimezoneNamesTable {
	var (
		NameColumn      = postgres.StringColumn("name")
		AbbrevColumn    = postgres.StringColumn("abbrev")
		UtcOffsetColumn = postgres.IntervalColumn("utc_offset")
		IsDstColumn     = postgres.BoolColumn("is_dst")
		allColumns      = postgres.ColumnList{NameColumn, AbbrevColumn, UtcOffsetColumn, IsDstColumn}
		mutableColumns  = postgres.ColumnList{NameColumn, AbbrevColumn, UtcOffsetColumn, IsDstColumn}
	)

	return pgTimezoneNamesTable{
		Table: postgres.NewTable(schemaName, tableName, allColumns...),

		//Columns
		Name:      NameColumn,
		Abbrev:    AbbrevColumn,
		UtcOffset: UtcOffsetColumn,
		IsDst:     IsDstColumn,

		AllColumns:     allColumns,
		MutableColumns: mutableColumns,
	}
}
