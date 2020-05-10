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

var PgForeignDataWrapper = newPgForeignDataWrapperTable()

type PgForeignDataWrapperTable struct {
	postgres.Table

	//Columns
	Oid          postgres.ColumnString
	Fdwname      postgres.ColumnString
	Fdwowner     postgres.ColumnString
	Fdwhandler   postgres.ColumnString
	Fdwvalidator postgres.ColumnString
	Fdwacl       postgres.ColumnString
	Fdwoptions   postgres.ColumnString

	AllColumns     postgres.ColumnList
	MutableColumns postgres.ColumnList
}

// creates new PgForeignDataWrapperTable with assigned alias
func (a *PgForeignDataWrapperTable) AS(alias string) *PgForeignDataWrapperTable {
	aliasTable := newPgForeignDataWrapperTable()

	aliasTable.Table.AS(alias)

	return aliasTable
}

func newPgForeignDataWrapperTable() *PgForeignDataWrapperTable {
	var (
		OidColumn          = postgres.StringColumn("oid")
		FdwnameColumn      = postgres.StringColumn("fdwname")
		FdwownerColumn     = postgres.StringColumn("fdwowner")
		FdwhandlerColumn   = postgres.StringColumn("fdwhandler")
		FdwvalidatorColumn = postgres.StringColumn("fdwvalidator")
		FdwaclColumn       = postgres.StringColumn("fdwacl")
		FdwoptionsColumn   = postgres.StringColumn("fdwoptions")
	)

	return &PgForeignDataWrapperTable{
		Table: postgres.NewTable("pg_catalog", "pg_foreign_data_wrapper", OidColumn, FdwnameColumn, FdwownerColumn, FdwhandlerColumn, FdwvalidatorColumn, FdwaclColumn, FdwoptionsColumn),

		//Columns
		Oid:          OidColumn,
		Fdwname:      FdwnameColumn,
		Fdwowner:     FdwownerColumn,
		Fdwhandler:   FdwhandlerColumn,
		Fdwvalidator: FdwvalidatorColumn,
		Fdwacl:       FdwaclColumn,
		Fdwoptions:   FdwoptionsColumn,

		AllColumns:     postgres.ColumnList{OidColumn, FdwnameColumn, FdwownerColumn, FdwhandlerColumn, FdwvalidatorColumn, FdwaclColumn, FdwoptionsColumn},
		MutableColumns: postgres.ColumnList{OidColumn, FdwnameColumn, FdwownerColumn, FdwhandlerColumn, FdwvalidatorColumn, FdwaclColumn, FdwoptionsColumn},
	}
}