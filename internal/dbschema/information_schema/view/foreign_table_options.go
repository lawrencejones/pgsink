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

var ForeignTableOptions = newForeignTableOptionsTable()

type foreignTableOptionsTable struct {
	postgres.Table

	//Columns
	ForeignTableCatalog postgres.ColumnString
	ForeignTableSchema  postgres.ColumnString
	ForeignTableName    postgres.ColumnString
	OptionName          postgres.ColumnString
	OptionValue         postgres.ColumnString

	AllColumns     postgres.ColumnList
	MutableColumns postgres.ColumnList
}

type ForeignTableOptionsTable struct {
	foreignTableOptionsTable

	EXCLUDED foreignTableOptionsTable
}

// AS creates new ForeignTableOptionsTable with assigned alias
func (a *ForeignTableOptionsTable) AS(alias string) *ForeignTableOptionsTable {
	aliasTable := newForeignTableOptionsTable()
	aliasTable.Table.AS(alias)
	return aliasTable
}

func newForeignTableOptionsTable() *ForeignTableOptionsTable {
	return &ForeignTableOptionsTable{
		foreignTableOptionsTable: newForeignTableOptionsTableImpl("information_schema", "foreign_table_options"),
		EXCLUDED:                 newForeignTableOptionsTableImpl("", "excluded"),
	}
}

func newForeignTableOptionsTableImpl(schemaName, tableName string) foreignTableOptionsTable {
	var (
		ForeignTableCatalogColumn = postgres.StringColumn("foreign_table_catalog")
		ForeignTableSchemaColumn  = postgres.StringColumn("foreign_table_schema")
		ForeignTableNameColumn    = postgres.StringColumn("foreign_table_name")
		OptionNameColumn          = postgres.StringColumn("option_name")
		OptionValueColumn         = postgres.StringColumn("option_value")
		allColumns                = postgres.ColumnList{ForeignTableCatalogColumn, ForeignTableSchemaColumn, ForeignTableNameColumn, OptionNameColumn, OptionValueColumn}
		mutableColumns            = postgres.ColumnList{ForeignTableCatalogColumn, ForeignTableSchemaColumn, ForeignTableNameColumn, OptionNameColumn, OptionValueColumn}
	)

	return foreignTableOptionsTable{
		Table: postgres.NewTable(schemaName, tableName, allColumns...),

		//Columns
		ForeignTableCatalog: ForeignTableCatalogColumn,
		ForeignTableSchema:  ForeignTableSchemaColumn,
		ForeignTableName:    ForeignTableNameColumn,
		OptionName:          OptionNameColumn,
		OptionValue:         OptionValueColumn,

		AllColumns:     allColumns,
		MutableColumns: mutableColumns,
	}
}
