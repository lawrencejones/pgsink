//
// Code generated by go-jet DO NOT EDIT.
// Generated at Tuesday, 12-May-20 07:59:32 BST
//
// WARNING: Changes to this file may cause incorrect behavior
// and will be lost if the code is regenerated
//

package model

import (
	"time"
)

type PgStatSubscription struct {
	Subid              *string
	Subname            *string
	Pid                *int32
	Relid              *string
	ReceivedLsn        *string
	LastMsgSendTime    *time.Time
	LastMsgReceiptTime *time.Time
	LatestEndLsn       *string
	LatestEndTime      *time.Time
}