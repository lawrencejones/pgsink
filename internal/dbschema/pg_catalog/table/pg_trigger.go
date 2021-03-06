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

var PgTrigger = newPgTriggerTable()

type pgTriggerTable struct {
	postgres.Table

	//Columns
	Oid            postgres.ColumnString
	Tgrelid        postgres.ColumnString
	Tgparentid     postgres.ColumnString
	Tgname         postgres.ColumnString
	Tgfoid         postgres.ColumnString
	Tgtype         postgres.ColumnInteger
	Tgenabled      postgres.ColumnString
	Tgisinternal   postgres.ColumnBool
	Tgconstrrelid  postgres.ColumnString
	Tgconstrindid  postgres.ColumnString
	Tgconstraint   postgres.ColumnString
	Tgdeferrable   postgres.ColumnBool
	Tginitdeferred postgres.ColumnBool
	Tgnargs        postgres.ColumnInteger
	Tgattr         postgres.ColumnString
	Tgargs         postgres.ColumnString
	Tgqual         postgres.ColumnString
	Tgoldtable     postgres.ColumnString
	Tgnewtable     postgres.ColumnString

	AllColumns     postgres.ColumnList
	MutableColumns postgres.ColumnList
}

type PgTriggerTable struct {
	pgTriggerTable

	EXCLUDED pgTriggerTable
}

// AS creates new PgTriggerTable with assigned alias
func (a *PgTriggerTable) AS(alias string) *PgTriggerTable {
	aliasTable := newPgTriggerTable()
	aliasTable.Table.AS(alias)
	return aliasTable
}

func newPgTriggerTable() *PgTriggerTable {
	return &PgTriggerTable{
		pgTriggerTable: newPgTriggerTableImpl("pg_catalog", "pg_trigger"),
		EXCLUDED:       newPgTriggerTableImpl("", "excluded"),
	}
}

func newPgTriggerTableImpl(schemaName, tableName string) pgTriggerTable {
	var (
		OidColumn            = postgres.StringColumn("oid")
		TgrelidColumn        = postgres.StringColumn("tgrelid")
		TgparentidColumn     = postgres.StringColumn("tgparentid")
		TgnameColumn         = postgres.StringColumn("tgname")
		TgfoidColumn         = postgres.StringColumn("tgfoid")
		TgtypeColumn         = postgres.IntegerColumn("tgtype")
		TgenabledColumn      = postgres.StringColumn("tgenabled")
		TgisinternalColumn   = postgres.BoolColumn("tgisinternal")
		TgconstrrelidColumn  = postgres.StringColumn("tgconstrrelid")
		TgconstrindidColumn  = postgres.StringColumn("tgconstrindid")
		TgconstraintColumn   = postgres.StringColumn("tgconstraint")
		TgdeferrableColumn   = postgres.BoolColumn("tgdeferrable")
		TginitdeferredColumn = postgres.BoolColumn("tginitdeferred")
		TgnargsColumn        = postgres.IntegerColumn("tgnargs")
		TgattrColumn         = postgres.StringColumn("tgattr")
		TgargsColumn         = postgres.StringColumn("tgargs")
		TgqualColumn         = postgres.StringColumn("tgqual")
		TgoldtableColumn     = postgres.StringColumn("tgoldtable")
		TgnewtableColumn     = postgres.StringColumn("tgnewtable")
		allColumns           = postgres.ColumnList{OidColumn, TgrelidColumn, TgparentidColumn, TgnameColumn, TgfoidColumn, TgtypeColumn, TgenabledColumn, TgisinternalColumn, TgconstrrelidColumn, TgconstrindidColumn, TgconstraintColumn, TgdeferrableColumn, TginitdeferredColumn, TgnargsColumn, TgattrColumn, TgargsColumn, TgqualColumn, TgoldtableColumn, TgnewtableColumn}
		mutableColumns       = postgres.ColumnList{OidColumn, TgrelidColumn, TgparentidColumn, TgnameColumn, TgfoidColumn, TgtypeColumn, TgenabledColumn, TgisinternalColumn, TgconstrrelidColumn, TgconstrindidColumn, TgconstraintColumn, TgdeferrableColumn, TginitdeferredColumn, TgnargsColumn, TgattrColumn, TgargsColumn, TgqualColumn, TgoldtableColumn, TgnewtableColumn}
	)

	return pgTriggerTable{
		Table: postgres.NewTable(schemaName, tableName, allColumns...),

		//Columns
		Oid:            OidColumn,
		Tgrelid:        TgrelidColumn,
		Tgparentid:     TgparentidColumn,
		Tgname:         TgnameColumn,
		Tgfoid:         TgfoidColumn,
		Tgtype:         TgtypeColumn,
		Tgenabled:      TgenabledColumn,
		Tgisinternal:   TgisinternalColumn,
		Tgconstrrelid:  TgconstrrelidColumn,
		Tgconstrindid:  TgconstrindidColumn,
		Tgconstraint:   TgconstraintColumn,
		Tgdeferrable:   TgdeferrableColumn,
		Tginitdeferred: TginitdeferredColumn,
		Tgnargs:        TgnargsColumn,
		Tgattr:         TgattrColumn,
		Tgargs:         TgargsColumn,
		Tgqual:         TgqualColumn,
		Tgoldtable:     TgoldtableColumn,
		Tgnewtable:     TgnewtableColumn,

		AllColumns:     allColumns,
		MutableColumns: mutableColumns,
	}
}
