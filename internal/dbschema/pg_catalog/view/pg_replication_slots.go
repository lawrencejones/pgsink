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

var PgReplicationSlots = newPgReplicationSlotsTable()

type pgReplicationSlotsTable struct {
	postgres.Table

	//Columns
	SlotName          postgres.ColumnString
	Plugin            postgres.ColumnString
	SlotType          postgres.ColumnString
	Datoid            postgres.ColumnString
	Database          postgres.ColumnString
	Temporary         postgres.ColumnBool
	Active            postgres.ColumnBool
	ActivePid         postgres.ColumnInteger
	Xmin              postgres.ColumnString
	CatalogXmin       postgres.ColumnString
	RestartLsn        postgres.ColumnString
	ConfirmedFlushLsn postgres.ColumnString
	WalStatus         postgres.ColumnString
	SafeWalSize       postgres.ColumnInteger

	AllColumns     postgres.ColumnList
	MutableColumns postgres.ColumnList
}

type PgReplicationSlotsTable struct {
	pgReplicationSlotsTable

	EXCLUDED pgReplicationSlotsTable
}

// AS creates new PgReplicationSlotsTable with assigned alias
func (a *PgReplicationSlotsTable) AS(alias string) *PgReplicationSlotsTable {
	aliasTable := newPgReplicationSlotsTable()
	aliasTable.Table.AS(alias)
	return aliasTable
}

func newPgReplicationSlotsTable() *PgReplicationSlotsTable {
	return &PgReplicationSlotsTable{
		pgReplicationSlotsTable: newPgReplicationSlotsTableImpl("pg_catalog", "pg_replication_slots"),
		EXCLUDED:                newPgReplicationSlotsTableImpl("", "excluded"),
	}
}

func newPgReplicationSlotsTableImpl(schemaName, tableName string) pgReplicationSlotsTable {
	var (
		SlotNameColumn          = postgres.StringColumn("slot_name")
		PluginColumn            = postgres.StringColumn("plugin")
		SlotTypeColumn          = postgres.StringColumn("slot_type")
		DatoidColumn            = postgres.StringColumn("datoid")
		DatabaseColumn          = postgres.StringColumn("database")
		TemporaryColumn         = postgres.BoolColumn("temporary")
		ActiveColumn            = postgres.BoolColumn("active")
		ActivePidColumn         = postgres.IntegerColumn("active_pid")
		XminColumn              = postgres.StringColumn("xmin")
		CatalogXminColumn       = postgres.StringColumn("catalog_xmin")
		RestartLsnColumn        = postgres.StringColumn("restart_lsn")
		ConfirmedFlushLsnColumn = postgres.StringColumn("confirmed_flush_lsn")
		WalStatusColumn         = postgres.StringColumn("wal_status")
		SafeWalSizeColumn       = postgres.IntegerColumn("safe_wal_size")
		allColumns              = postgres.ColumnList{SlotNameColumn, PluginColumn, SlotTypeColumn, DatoidColumn, DatabaseColumn, TemporaryColumn, ActiveColumn, ActivePidColumn, XminColumn, CatalogXminColumn, RestartLsnColumn, ConfirmedFlushLsnColumn, WalStatusColumn, SafeWalSizeColumn}
		mutableColumns          = postgres.ColumnList{SlotNameColumn, PluginColumn, SlotTypeColumn, DatoidColumn, DatabaseColumn, TemporaryColumn, ActiveColumn, ActivePidColumn, XminColumn, CatalogXminColumn, RestartLsnColumn, ConfirmedFlushLsnColumn, WalStatusColumn, SafeWalSizeColumn}
	)

	return pgReplicationSlotsTable{
		Table: postgres.NewTable(schemaName, tableName, allColumns...),

		//Columns
		SlotName:          SlotNameColumn,
		Plugin:            PluginColumn,
		SlotType:          SlotTypeColumn,
		Datoid:            DatoidColumn,
		Database:          DatabaseColumn,
		Temporary:         TemporaryColumn,
		Active:            ActiveColumn,
		ActivePid:         ActivePidColumn,
		Xmin:              XminColumn,
		CatalogXmin:       CatalogXminColumn,
		RestartLsn:        RestartLsnColumn,
		ConfirmedFlushLsn: ConfirmedFlushLsnColumn,
		WalStatus:         WalStatusColumn,
		SafeWalSize:       SafeWalSizeColumn,

		AllColumns:     allColumns,
		MutableColumns: mutableColumns,
	}
}
