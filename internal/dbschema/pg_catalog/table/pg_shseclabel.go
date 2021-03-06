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

var PgShseclabel = newPgShseclabelTable()

type pgShseclabelTable struct {
	postgres.Table

	//Columns
	Objoid   postgres.ColumnString
	Classoid postgres.ColumnString
	Provider postgres.ColumnString
	Label    postgres.ColumnString

	AllColumns     postgres.ColumnList
	MutableColumns postgres.ColumnList
}

type PgShseclabelTable struct {
	pgShseclabelTable

	EXCLUDED pgShseclabelTable
}

// AS creates new PgShseclabelTable with assigned alias
func (a *PgShseclabelTable) AS(alias string) *PgShseclabelTable {
	aliasTable := newPgShseclabelTable()
	aliasTable.Table.AS(alias)
	return aliasTable
}

func newPgShseclabelTable() *PgShseclabelTable {
	return &PgShseclabelTable{
		pgShseclabelTable: newPgShseclabelTableImpl("pg_catalog", "pg_shseclabel"),
		EXCLUDED:          newPgShseclabelTableImpl("", "excluded"),
	}
}

func newPgShseclabelTableImpl(schemaName, tableName string) pgShseclabelTable {
	var (
		ObjoidColumn   = postgres.StringColumn("objoid")
		ClassoidColumn = postgres.StringColumn("classoid")
		ProviderColumn = postgres.StringColumn("provider")
		LabelColumn    = postgres.StringColumn("label")
		allColumns     = postgres.ColumnList{ObjoidColumn, ClassoidColumn, ProviderColumn, LabelColumn}
		mutableColumns = postgres.ColumnList{ObjoidColumn, ClassoidColumn, ProviderColumn, LabelColumn}
	)

	return pgShseclabelTable{
		Table: postgres.NewTable(schemaName, tableName, allColumns...),

		//Columns
		Objoid:   ObjoidColumn,
		Classoid: ClassoidColumn,
		Provider: ProviderColumn,
		Label:    LabelColumn,

		AllColumns:     allColumns,
		MutableColumns: mutableColumns,
	}
}
