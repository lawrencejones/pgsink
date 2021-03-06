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

var PgAvailableExtensionVersions = newPgAvailableExtensionVersionsTable()

type pgAvailableExtensionVersionsTable struct {
	postgres.Table

	//Columns
	Name        postgres.ColumnString
	Version     postgres.ColumnString
	Installed   postgres.ColumnBool
	Superuser   postgres.ColumnBool
	Trusted     postgres.ColumnBool
	Relocatable postgres.ColumnBool
	Schema      postgres.ColumnString
	Requires    postgres.ColumnString
	Comment     postgres.ColumnString

	AllColumns     postgres.ColumnList
	MutableColumns postgres.ColumnList
}

type PgAvailableExtensionVersionsTable struct {
	pgAvailableExtensionVersionsTable

	EXCLUDED pgAvailableExtensionVersionsTable
}

// AS creates new PgAvailableExtensionVersionsTable with assigned alias
func (a *PgAvailableExtensionVersionsTable) AS(alias string) *PgAvailableExtensionVersionsTable {
	aliasTable := newPgAvailableExtensionVersionsTable()
	aliasTable.Table.AS(alias)
	return aliasTable
}

func newPgAvailableExtensionVersionsTable() *PgAvailableExtensionVersionsTable {
	return &PgAvailableExtensionVersionsTable{
		pgAvailableExtensionVersionsTable: newPgAvailableExtensionVersionsTableImpl("pg_catalog", "pg_available_extension_versions"),
		EXCLUDED:                          newPgAvailableExtensionVersionsTableImpl("", "excluded"),
	}
}

func newPgAvailableExtensionVersionsTableImpl(schemaName, tableName string) pgAvailableExtensionVersionsTable {
	var (
		NameColumn        = postgres.StringColumn("name")
		VersionColumn     = postgres.StringColumn("version")
		InstalledColumn   = postgres.BoolColumn("installed")
		SuperuserColumn   = postgres.BoolColumn("superuser")
		TrustedColumn     = postgres.BoolColumn("trusted")
		RelocatableColumn = postgres.BoolColumn("relocatable")
		SchemaColumn      = postgres.StringColumn("schema")
		RequiresColumn    = postgres.StringColumn("requires")
		CommentColumn     = postgres.StringColumn("comment")
		allColumns        = postgres.ColumnList{NameColumn, VersionColumn, InstalledColumn, SuperuserColumn, TrustedColumn, RelocatableColumn, SchemaColumn, RequiresColumn, CommentColumn}
		mutableColumns    = postgres.ColumnList{NameColumn, VersionColumn, InstalledColumn, SuperuserColumn, TrustedColumn, RelocatableColumn, SchemaColumn, RequiresColumn, CommentColumn}
	)

	return pgAvailableExtensionVersionsTable{
		Table: postgres.NewTable(schemaName, tableName, allColumns...),

		//Columns
		Name:        NameColumn,
		Version:     VersionColumn,
		Installed:   InstalledColumn,
		Superuser:   SuperuserColumn,
		Trusted:     TrustedColumn,
		Relocatable: RelocatableColumn,
		Schema:      SchemaColumn,
		Requires:    RequiresColumn,
		Comment:     CommentColumn,

		AllColumns:     allColumns,
		MutableColumns: mutableColumns,
	}
}
