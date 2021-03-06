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

var PgStatProgressCreateIndex = newPgStatProgressCreateIndexTable()

type pgStatProgressCreateIndexTable struct {
	postgres.Table

	//Columns
	Pid              postgres.ColumnInteger
	Datid            postgres.ColumnString
	Datname          postgres.ColumnString
	Relid            postgres.ColumnString
	IndexRelid       postgres.ColumnString
	Command          postgres.ColumnString
	Phase            postgres.ColumnString
	LockersTotal     postgres.ColumnInteger
	LockersDone      postgres.ColumnInteger
	CurrentLockerPid postgres.ColumnInteger
	BlocksTotal      postgres.ColumnInteger
	BlocksDone       postgres.ColumnInteger
	TuplesTotal      postgres.ColumnInteger
	TuplesDone       postgres.ColumnInteger
	PartitionsTotal  postgres.ColumnInteger
	PartitionsDone   postgres.ColumnInteger

	AllColumns     postgres.ColumnList
	MutableColumns postgres.ColumnList
}

type PgStatProgressCreateIndexTable struct {
	pgStatProgressCreateIndexTable

	EXCLUDED pgStatProgressCreateIndexTable
}

// AS creates new PgStatProgressCreateIndexTable with assigned alias
func (a *PgStatProgressCreateIndexTable) AS(alias string) *PgStatProgressCreateIndexTable {
	aliasTable := newPgStatProgressCreateIndexTable()
	aliasTable.Table.AS(alias)
	return aliasTable
}

func newPgStatProgressCreateIndexTable() *PgStatProgressCreateIndexTable {
	return &PgStatProgressCreateIndexTable{
		pgStatProgressCreateIndexTable: newPgStatProgressCreateIndexTableImpl("pg_catalog", "pg_stat_progress_create_index"),
		EXCLUDED:                       newPgStatProgressCreateIndexTableImpl("", "excluded"),
	}
}

func newPgStatProgressCreateIndexTableImpl(schemaName, tableName string) pgStatProgressCreateIndexTable {
	var (
		PidColumn              = postgres.IntegerColumn("pid")
		DatidColumn            = postgres.StringColumn("datid")
		DatnameColumn          = postgres.StringColumn("datname")
		RelidColumn            = postgres.StringColumn("relid")
		IndexRelidColumn       = postgres.StringColumn("index_relid")
		CommandColumn          = postgres.StringColumn("command")
		PhaseColumn            = postgres.StringColumn("phase")
		LockersTotalColumn     = postgres.IntegerColumn("lockers_total")
		LockersDoneColumn      = postgres.IntegerColumn("lockers_done")
		CurrentLockerPidColumn = postgres.IntegerColumn("current_locker_pid")
		BlocksTotalColumn      = postgres.IntegerColumn("blocks_total")
		BlocksDoneColumn       = postgres.IntegerColumn("blocks_done")
		TuplesTotalColumn      = postgres.IntegerColumn("tuples_total")
		TuplesDoneColumn       = postgres.IntegerColumn("tuples_done")
		PartitionsTotalColumn  = postgres.IntegerColumn("partitions_total")
		PartitionsDoneColumn   = postgres.IntegerColumn("partitions_done")
		allColumns             = postgres.ColumnList{PidColumn, DatidColumn, DatnameColumn, RelidColumn, IndexRelidColumn, CommandColumn, PhaseColumn, LockersTotalColumn, LockersDoneColumn, CurrentLockerPidColumn, BlocksTotalColumn, BlocksDoneColumn, TuplesTotalColumn, TuplesDoneColumn, PartitionsTotalColumn, PartitionsDoneColumn}
		mutableColumns         = postgres.ColumnList{PidColumn, DatidColumn, DatnameColumn, RelidColumn, IndexRelidColumn, CommandColumn, PhaseColumn, LockersTotalColumn, LockersDoneColumn, CurrentLockerPidColumn, BlocksTotalColumn, BlocksDoneColumn, TuplesTotalColumn, TuplesDoneColumn, PartitionsTotalColumn, PartitionsDoneColumn}
	)

	return pgStatProgressCreateIndexTable{
		Table: postgres.NewTable(schemaName, tableName, allColumns...),

		//Columns
		Pid:              PidColumn,
		Datid:            DatidColumn,
		Datname:          DatnameColumn,
		Relid:            RelidColumn,
		IndexRelid:       IndexRelidColumn,
		Command:          CommandColumn,
		Phase:            PhaseColumn,
		LockersTotal:     LockersTotalColumn,
		LockersDone:      LockersDoneColumn,
		CurrentLockerPid: CurrentLockerPidColumn,
		BlocksTotal:      BlocksTotalColumn,
		BlocksDone:       BlocksDoneColumn,
		TuplesTotal:      TuplesTotalColumn,
		TuplesDone:       TuplesDoneColumn,
		PartitionsTotal:  PartitionsTotalColumn,
		PartitionsDone:   PartitionsDoneColumn,

		AllColumns:     allColumns,
		MutableColumns: mutableColumns,
	}
}
