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

var PgStatSubscription = newPgStatSubscriptionTable()

type pgStatSubscriptionTable struct {
	postgres.Table

	//Columns
	Subid              postgres.ColumnString
	Subname            postgres.ColumnString
	Pid                postgres.ColumnInteger
	Relid              postgres.ColumnString
	ReceivedLsn        postgres.ColumnString
	LastMsgSendTime    postgres.ColumnTimestampz
	LastMsgReceiptTime postgres.ColumnTimestampz
	LatestEndLsn       postgres.ColumnString
	LatestEndTime      postgres.ColumnTimestampz

	AllColumns     postgres.ColumnList
	MutableColumns postgres.ColumnList
}

type PgStatSubscriptionTable struct {
	pgStatSubscriptionTable

	EXCLUDED pgStatSubscriptionTable
}

// AS creates new PgStatSubscriptionTable with assigned alias
func (a *PgStatSubscriptionTable) AS(alias string) *PgStatSubscriptionTable {
	aliasTable := newPgStatSubscriptionTable()
	aliasTable.Table.AS(alias)
	return aliasTable
}

func newPgStatSubscriptionTable() *PgStatSubscriptionTable {
	return &PgStatSubscriptionTable{
		pgStatSubscriptionTable: newPgStatSubscriptionTableImpl("pg_catalog", "pg_stat_subscription"),
		EXCLUDED:                newPgStatSubscriptionTableImpl("", "excluded"),
	}
}

func newPgStatSubscriptionTableImpl(schemaName, tableName string) pgStatSubscriptionTable {
	var (
		SubidColumn              = postgres.StringColumn("subid")
		SubnameColumn            = postgres.StringColumn("subname")
		PidColumn                = postgres.IntegerColumn("pid")
		RelidColumn              = postgres.StringColumn("relid")
		ReceivedLsnColumn        = postgres.StringColumn("received_lsn")
		LastMsgSendTimeColumn    = postgres.TimestampzColumn("last_msg_send_time")
		LastMsgReceiptTimeColumn = postgres.TimestampzColumn("last_msg_receipt_time")
		LatestEndLsnColumn       = postgres.StringColumn("latest_end_lsn")
		LatestEndTimeColumn      = postgres.TimestampzColumn("latest_end_time")
		allColumns               = postgres.ColumnList{SubidColumn, SubnameColumn, PidColumn, RelidColumn, ReceivedLsnColumn, LastMsgSendTimeColumn, LastMsgReceiptTimeColumn, LatestEndLsnColumn, LatestEndTimeColumn}
		mutableColumns           = postgres.ColumnList{SubidColumn, SubnameColumn, PidColumn, RelidColumn, ReceivedLsnColumn, LastMsgSendTimeColumn, LastMsgReceiptTimeColumn, LatestEndLsnColumn, LatestEndTimeColumn}
	)

	return pgStatSubscriptionTable{
		Table: postgres.NewTable(schemaName, tableName, allColumns...),

		//Columns
		Subid:              SubidColumn,
		Subname:            SubnameColumn,
		Pid:                PidColumn,
		Relid:              RelidColumn,
		ReceivedLsn:        ReceivedLsnColumn,
		LastMsgSendTime:    LastMsgSendTimeColumn,
		LastMsgReceiptTime: LastMsgReceiptTimeColumn,
		LatestEndLsn:       LatestEndLsnColumn,
		LatestEndTime:      LatestEndTimeColumn,

		AllColumns:     allColumns,
		MutableColumns: mutableColumns,
	}
}
