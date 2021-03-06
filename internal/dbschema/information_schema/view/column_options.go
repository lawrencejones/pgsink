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

var ColumnOptions = newColumnOptionsTable()

type columnOptionsTable struct {
	postgres.Table

	//Columns
	TableCatalog postgres.ColumnString
	TableSchema  postgres.ColumnString
	TableName    postgres.ColumnString
	ColumnName   postgres.ColumnString
	OptionName   postgres.ColumnString
	OptionValue  postgres.ColumnString

	AllColumns     postgres.ColumnList
	MutableColumns postgres.ColumnList
}

type ColumnOptionsTable struct {
	columnOptionsTable

	EXCLUDED columnOptionsTable
}

// AS creates new ColumnOptionsTable with assigned alias
func (a *ColumnOptionsTable) AS(alias string) *ColumnOptionsTable {
	aliasTable := newColumnOptionsTable()
	aliasTable.Table.AS(alias)
	return aliasTable
}

func newColumnOptionsTable() *ColumnOptionsTable {
	return &ColumnOptionsTable{
		columnOptionsTable: newColumnOptionsTableImpl("information_schema", "column_options"),
		EXCLUDED:           newColumnOptionsTableImpl("", "excluded"),
	}
}

func newColumnOptionsTableImpl(schemaName, tableName string) columnOptionsTable {
	var (
		TableCatalogColumn = postgres.StringColumn("table_catalog")
		TableSchemaColumn  = postgres.StringColumn("table_schema")
		TableNameColumn    = postgres.StringColumn("table_name")
		ColumnNameColumn   = postgres.StringColumn("column_name")
		OptionNameColumn   = postgres.StringColumn("option_name")
		OptionValueColumn  = postgres.StringColumn("option_value")
		allColumns         = postgres.ColumnList{TableCatalogColumn, TableSchemaColumn, TableNameColumn, ColumnNameColumn, OptionNameColumn, OptionValueColumn}
		mutableColumns     = postgres.ColumnList{TableCatalogColumn, TableSchemaColumn, TableNameColumn, ColumnNameColumn, OptionNameColumn, OptionValueColumn}
	)

	return columnOptionsTable{
		Table: postgres.NewTable(schemaName, tableName, allColumns...),

		//Columns
		TableCatalog: TableCatalogColumn,
		TableSchema:  TableSchemaColumn,
		TableName:    TableNameColumn,
		ColumnName:   ColumnNameColumn,
		OptionName:   OptionNameColumn,
		OptionValue:  OptionValueColumn,

		AllColumns:     allColumns,
		MutableColumns: mutableColumns,
	}
}
