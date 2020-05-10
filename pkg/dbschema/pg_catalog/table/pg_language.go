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

var PgLanguage = newPgLanguageTable()

type PgLanguageTable struct {
	postgres.Table

	//Columns
	Oid           postgres.ColumnString
	Lanname       postgres.ColumnString
	Lanowner      postgres.ColumnString
	Lanispl       postgres.ColumnBool
	Lanpltrusted  postgres.ColumnBool
	Lanplcallfoid postgres.ColumnString
	Laninline     postgres.ColumnString
	Lanvalidator  postgres.ColumnString
	Lanacl        postgres.ColumnString

	AllColumns     postgres.ColumnList
	MutableColumns postgres.ColumnList
}

// creates new PgLanguageTable with assigned alias
func (a *PgLanguageTable) AS(alias string) *PgLanguageTable {
	aliasTable := newPgLanguageTable()

	aliasTable.Table.AS(alias)

	return aliasTable
}

func newPgLanguageTable() *PgLanguageTable {
	var (
		OidColumn           = postgres.StringColumn("oid")
		LannameColumn       = postgres.StringColumn("lanname")
		LanownerColumn      = postgres.StringColumn("lanowner")
		LanisplColumn       = postgres.BoolColumn("lanispl")
		LanpltrustedColumn  = postgres.BoolColumn("lanpltrusted")
		LanplcallfoidColumn = postgres.StringColumn("lanplcallfoid")
		LaninlineColumn     = postgres.StringColumn("laninline")
		LanvalidatorColumn  = postgres.StringColumn("lanvalidator")
		LanaclColumn        = postgres.StringColumn("lanacl")
	)

	return &PgLanguageTable{
		Table: postgres.NewTable("pg_catalog", "pg_language", OidColumn, LannameColumn, LanownerColumn, LanisplColumn, LanpltrustedColumn, LanplcallfoidColumn, LaninlineColumn, LanvalidatorColumn, LanaclColumn),

		//Columns
		Oid:           OidColumn,
		Lanname:       LannameColumn,
		Lanowner:      LanownerColumn,
		Lanispl:       LanisplColumn,
		Lanpltrusted:  LanpltrustedColumn,
		Lanplcallfoid: LanplcallfoidColumn,
		Laninline:     LaninlineColumn,
		Lanvalidator:  LanvalidatorColumn,
		Lanacl:        LanaclColumn,

		AllColumns:     postgres.ColumnList{OidColumn, LannameColumn, LanownerColumn, LanisplColumn, LanpltrustedColumn, LanplcallfoidColumn, LaninlineColumn, LanvalidatorColumn, LanaclColumn},
		MutableColumns: postgres.ColumnList{OidColumn, LannameColumn, LanownerColumn, LanisplColumn, LanpltrustedColumn, LanplcallfoidColumn, LaninlineColumn, LanvalidatorColumn, LanaclColumn},
	}
}