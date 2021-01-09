//
// Code generated by go-jet DO NOT EDIT.
// Generated at Tuesday, 12-May-20 09:15:06 BST
//
// WARNING: Changes to this file may cause incorrect behavior
// and will be lost if the code is regenerated
//

package view

import (
	"github.com/go-jet/jet/postgres"
)

var CheckConstraints = newCheckConstraintsTable()

type CheckConstraintsTable struct {
	postgres.Table

	//Columns
	ConstraintCatalog postgres.ColumnString
	ConstraintSchema  postgres.ColumnString
	ConstraintName    postgres.ColumnString
	CheckClause       postgres.ColumnString

	AllColumns     postgres.ColumnList
	MutableColumns postgres.ColumnList
}

// creates new CheckConstraintsTable with assigned alias
func (a *CheckConstraintsTable) AS(alias string) *CheckConstraintsTable {
	aliasTable := newCheckConstraintsTable()

	aliasTable.Table.AS(alias)

	return aliasTable
}

func newCheckConstraintsTable() *CheckConstraintsTable {
	var (
		ConstraintCatalogColumn = postgres.StringColumn("constraint_catalog")
		ConstraintSchemaColumn  = postgres.StringColumn("constraint_schema")
		ConstraintNameColumn    = postgres.StringColumn("constraint_name")
		CheckClauseColumn       = postgres.StringColumn("check_clause")
	)

	return &CheckConstraintsTable{
		Table: postgres.NewTable("information_schema", "check_constraints", ConstraintCatalogColumn, ConstraintSchemaColumn, ConstraintNameColumn, CheckClauseColumn),

		//Columns
		ConstraintCatalog: ConstraintCatalogColumn,
		ConstraintSchema:  ConstraintSchemaColumn,
		ConstraintName:    ConstraintNameColumn,
		CheckClause:       CheckClauseColumn,

		AllColumns:     postgres.ColumnList{ConstraintCatalogColumn, ConstraintSchemaColumn, ConstraintNameColumn, CheckClauseColumn},
		MutableColumns: postgres.ColumnList{ConstraintCatalogColumn, ConstraintSchemaColumn, ConstraintNameColumn, CheckClauseColumn},
	}
}