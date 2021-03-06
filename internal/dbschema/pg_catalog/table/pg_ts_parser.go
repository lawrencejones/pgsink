//
// Code generated by go-jet DO NOT EDIT.
//
// WARNING: Changes to this file may cause incorrect behavior
// and will be lost if the code is regenerated
//

package table

import (
	"github.com/go-jet/jet/v2/postgres"
)

var PgTsParser = newPgTsParserTable()

type pgTsParserTable struct {
	postgres.Table

	//Columns
	Oid          postgres.ColumnString
	Prsname      postgres.ColumnString
	Prsnamespace postgres.ColumnString
	Prsstart     postgres.ColumnString
	Prstoken     postgres.ColumnString
	Prsend       postgres.ColumnString
	Prsheadline  postgres.ColumnString
	Prslextype   postgres.ColumnString

	AllColumns     postgres.ColumnList
	MutableColumns postgres.ColumnList
}

type PgTsParserTable struct {
	pgTsParserTable

	EXCLUDED pgTsParserTable
}

// AS creates new PgTsParserTable with assigned alias
func (a *PgTsParserTable) AS(alias string) *PgTsParserTable {
	aliasTable := newPgTsParserTable()
	aliasTable.Table.AS(alias)
	return aliasTable
}

func newPgTsParserTable() *PgTsParserTable {
	return &PgTsParserTable{
		pgTsParserTable: newPgTsParserTableImpl("pg_catalog", "pg_ts_parser"),
		EXCLUDED:        newPgTsParserTableImpl("", "excluded"),
	}
}

func newPgTsParserTableImpl(schemaName, tableName string) pgTsParserTable {
	var (
		OidColumn          = postgres.StringColumn("oid")
		PrsnameColumn      = postgres.StringColumn("prsname")
		PrsnamespaceColumn = postgres.StringColumn("prsnamespace")
		PrsstartColumn     = postgres.StringColumn("prsstart")
		PrstokenColumn     = postgres.StringColumn("prstoken")
		PrsendColumn       = postgres.StringColumn("prsend")
		PrsheadlineColumn  = postgres.StringColumn("prsheadline")
		PrslextypeColumn   = postgres.StringColumn("prslextype")
		allColumns         = postgres.ColumnList{OidColumn, PrsnameColumn, PrsnamespaceColumn, PrsstartColumn, PrstokenColumn, PrsendColumn, PrsheadlineColumn, PrslextypeColumn}
		mutableColumns     = postgres.ColumnList{OidColumn, PrsnameColumn, PrsnamespaceColumn, PrsstartColumn, PrstokenColumn, PrsendColumn, PrsheadlineColumn, PrslextypeColumn}
	)

	return pgTsParserTable{
		Table: postgres.NewTable(schemaName, tableName, allColumns...),

		//Columns
		Oid:          OidColumn,
		Prsname:      PrsnameColumn,
		Prsnamespace: PrsnamespaceColumn,
		Prsstart:     PrsstartColumn,
		Prstoken:     PrstokenColumn,
		Prsend:       PrsendColumn,
		Prsheadline:  PrsheadlineColumn,
		Prslextype:   PrslextypeColumn,

		AllColumns:     allColumns,
		MutableColumns: mutableColumns,
	}
}
