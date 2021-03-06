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

var ForeignTables = newForeignTablesTable()

type foreignTablesTable struct {
	postgres.Table

	//Columns
	ForeignTableCatalog  postgres.ColumnString
	ForeignTableSchema   postgres.ColumnString
	ForeignTableName     postgres.ColumnString
	ForeignServerCatalog postgres.ColumnString
	ForeignServerName    postgres.ColumnString

	AllColumns     postgres.ColumnList
	MutableColumns postgres.ColumnList
}

type ForeignTablesTable struct {
	foreignTablesTable

	EXCLUDED foreignTablesTable
}

// AS creates new ForeignTablesTable with assigned alias
func (a *ForeignTablesTable) AS(alias string) *ForeignTablesTable {
	aliasTable := newForeignTablesTable()
	aliasTable.Table.AS(alias)
	return aliasTable
}

func newForeignTablesTable() *ForeignTablesTable {
	return &ForeignTablesTable{
		foreignTablesTable: newForeignTablesTableImpl("information_schema", "foreign_tables"),
		EXCLUDED:           newForeignTablesTableImpl("", "excluded"),
	}
}

func newForeignTablesTableImpl(schemaName, tableName string) foreignTablesTable {
	var (
		ForeignTableCatalogColumn  = postgres.StringColumn("foreign_table_catalog")
		ForeignTableSchemaColumn   = postgres.StringColumn("foreign_table_schema")
		ForeignTableNameColumn     = postgres.StringColumn("foreign_table_name")
		ForeignServerCatalogColumn = postgres.StringColumn("foreign_server_catalog")
		ForeignServerNameColumn    = postgres.StringColumn("foreign_server_name")
		allColumns                 = postgres.ColumnList{ForeignTableCatalogColumn, ForeignTableSchemaColumn, ForeignTableNameColumn, ForeignServerCatalogColumn, ForeignServerNameColumn}
		mutableColumns             = postgres.ColumnList{ForeignTableCatalogColumn, ForeignTableSchemaColumn, ForeignTableNameColumn, ForeignServerCatalogColumn, ForeignServerNameColumn}
	)

	return foreignTablesTable{
		Table: postgres.NewTable(schemaName, tableName, allColumns...),

		//Columns
		ForeignTableCatalog:  ForeignTableCatalogColumn,
		ForeignTableSchema:   ForeignTableSchemaColumn,
		ForeignTableName:     ForeignTableNameColumn,
		ForeignServerCatalog: ForeignServerCatalogColumn,
		ForeignServerName:    ForeignServerNameColumn,

		AllColumns:     allColumns,
		MutableColumns: mutableColumns,
	}
}
