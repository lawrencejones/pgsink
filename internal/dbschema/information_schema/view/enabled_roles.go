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

var EnabledRoles = newEnabledRolesTable()

type enabledRolesTable struct {
	postgres.Table

	//Columns
	RoleName postgres.ColumnString

	AllColumns     postgres.ColumnList
	MutableColumns postgres.ColumnList
}

type EnabledRolesTable struct {
	enabledRolesTable

	EXCLUDED enabledRolesTable
}

// AS creates new EnabledRolesTable with assigned alias
func (a *EnabledRolesTable) AS(alias string) *EnabledRolesTable {
	aliasTable := newEnabledRolesTable()
	aliasTable.Table.AS(alias)
	return aliasTable
}

func newEnabledRolesTable() *EnabledRolesTable {
	return &EnabledRolesTable{
		enabledRolesTable: newEnabledRolesTableImpl("information_schema", "enabled_roles"),
		EXCLUDED:          newEnabledRolesTableImpl("", "excluded"),
	}
}

func newEnabledRolesTableImpl(schemaName, tableName string) enabledRolesTable {
	var (
		RoleNameColumn = postgres.StringColumn("role_name")
		allColumns     = postgres.ColumnList{RoleNameColumn}
		mutableColumns = postgres.ColumnList{RoleNameColumn}
	)

	return enabledRolesTable{
		Table: postgres.NewTable(schemaName, tableName, allColumns...),

		//Columns
		RoleName: RoleNameColumn,

		AllColumns:     allColumns,
		MutableColumns: mutableColumns,
	}
}
