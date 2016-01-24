// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package plans

import (
	"fmt"
	"sort"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/column"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/perfschema"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/format"
	"github.com/pingcap/tidb/util/types"
)

var _ = (*PerfSchemaPlan)(nil)

// PerfSchemaPlan handles performance_schema query, simulates the behavior of
// MySQL.
type PerfSchemaPlan struct {
	TableName string
	rows      []*plan.Row
	cursor    int
}

var (
	setupActorsFields               []*field.ResultField
	setupObjectsFields              []*field.ResultField
	setupInstrsFields               []*field.ResultField
	setupConsumersFields            []*field.ResultField
	setupTimersFields               []*field.ResultField
	stmtsCurrentFields              []*field.ResultField
	stmtsHistoryFields              []*field.ResultField
	stmtsHistoryLongFields          []*field.ResultField
	preparedStmtsInstancesFields    []*field.ResultField
	transCurrentFields              []*field.ResultField
	transHistoryFields              []*field.ResultField
	transHistoryLongFields          []*field.ResultField
	stagesCurrentFields             []*field.ResultField
	stagesHistoryFields             []*field.ResultField
	stagesHistoryLongFields         []*field.ResultField
	setupActorsRecords              [][]interface{}
	setupObjectsRecords             [][]interface{}
	setupInstrsRecords              [][]interface{}
	setupConsumersRecords           [][]interface{}
	setupTimersRecords              [][]interface{}
)

const (
	tableSetupActors        = "setup_actors"
	tableSetupObjects       = "setup_objects"
	tableSetupInstrs        = "setup_instruments"
	tableSetupConsumers     = "setup_consumers"
	tableSetupTimers        = "setup_timers"
	tableStmtsCurrent       = "events_statements_current"
	tableStmtsHistory       = "events_statements_history"
	tableStmtsHistoryLong   = "events_statements_history_long"
	tablePrepStmtsInstances = "prepared_statements_instances"
	tableTransCurrent       = "events_transactions_current"
	tableTransHistory       = "events_transactions_history"
	tableTransHistoryLong   = "events_transactions_history_long"
	tableStagesCurrent      = "events_stages_current"
	tableStagesHistory      = "events_stages_history"
	tableStagesHistoryLong  = "events_stages_history_long"
)

// NewPerfSchemaPlan returns new PerfSchemaPlan instance, and checks if the
// given table name is valid.
func NewPerfSchemaPlan(tableName string) (isp *PerfSchemaPlan, err error) {
	switch strings.ToUpper(tableName) {
	case tableSetupActors:
	case tableSetupObjects:
	case tableSetupInstrs:
	case tableSetupConsumers:
	case tableSetupTimers:
	case tableStmtsCurrent:
	case tableStmtsHistory:
	case tableStmtsHistoryLong:
	case tablePrepStmtsInstances:
	case tableTransCurrent:
	case tableTransHistory:
	case tableTransHistoryLong:
	case tableStagesCurrent:
	case tableStagesHistory:
	case tableStagesHistoryLong:
	default:
		return nil, errors.Errorf("table PERFORMANCE_SCHEMA.%s does not exist", tableName)
	}
	isp = &PerfSchemaPlan{
		TableName: strings.ToUpper(tableName),
	}
	return
}

// CREATE TABLE if not exists performance_schema.setup_actors (
// 		HOST			CHAR(60) NOT NULL  DEFAULT '%',
// 		USER			CHAR(32) NOT NULL  DEFAULT '%',
// 		ROLE			CHAR(16) NOT NULL  DEFAULT '%',
// 		ENABLED			ENUM('YES','NO') NOT NULL  DEFAULT 'YES',
// 		HISTORY			ENUM('YES','NO') NOT NULL  DEFAULT 'YES');
func buildResultFieldsForSetupActors() (rfs []*field.ResultField) {
	tbName := tableSetupActors
	rfs = append(rfs, buildResultField(tbName, "HOST", mysql.TypeString, 60, mysql.NotNullFlag, "%"))
	rfs = append(rfs, buildResultField(tbName, "USER", mysql.TypeString, 32, mysql.NotNullFlag, "%"))
	rfs = append(rfs, buildResultField(tbName, "ROLE", mysql.TypeString, 16, mysql.NotNullFlag, "%"))
	rfs = append(rfs, buildEnumResultField(tbName, "ENABLED", []string{"YES", "NO"}, mysql.NotNullFlag, "YES"))
	rfs = append(rfs, buildEnumResultField(tbName, "HISTORY", []string{"YES", "NO"}, mysql.NotNullFlag, "YES"))
	return rfs
}

// CREATE TABLE if not exists performance_schema.setup_objects (
// 		OBJECT_TYPE		ENUM('EVENT','FUNCTION','TABLE') NOT NULL  DEFAULT 'TABLE',
// 		OBJECT_SCHEMA	VARCHAR(64)  DEFAULT '%',
// 		OBJECT_NAME		VARCHAR(64) NOT NULL  DEFAULT '%',
// 		ENABLED			ENUM('YES','NO') NOT NULL  DEFAULT 'YES',
// 		TIMED			ENUM('YES','NO') NOT NULL  DEFAULT 'YES');
func buildResultFieldsForSetupObjects() (rfs []*field.ResultField) {
	tbName := tableSetupObjects
	rfs = append(rfs, buildEnumResultField(tbName, "OBJECT_TYPE", []string{"EVENT", "FUNCTION", "TABLE"}, mysql.NotNullFlag, "TABLE"))
	rfs = append(rfs, buildResultField(tbName, "OBJECT_SCHEMA", mysql.TypeVarChar, 64, 0, "%"))
	rfs = append(rfs, buildResultField(tbName, "OBJECT_NAME", mysql.TypeVarChar, 64, mysql.NotNullFlag, "%"))
	rfs = append(rfs, buildEnumResultField(tbName, "ENABLED", []string{"YES", "NO"}, mysql.NotNullFlag, "YES"))
	rfs = append(rfs, buildEnumResultField(tbName, "TIMED", []string{"YES", "NO"}, mysql.NotNullFlag, "YES"))
	return rfs
}

// CREATE TABLE if not exists performance_schema.setup_instruments (
// 		NAME			VARCHAR(128) NOT NULL,
// 		ENABLED			ENUM('YES','NO') NOT NULL,
// 		TIMED			ENUM('YES','NO') NOT NULL);
func buildResultFieldsForSetupInstrs() (rfs []*field.ResultField) {
	tbName := tableSetupInstrs
	rfs = append(rfs, buildResultField(tbName, "NAME", mysql.TypeVarChar, 128, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildEnumResultField(tbName, "ENABLED", []string{"YES", "NO"}, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildEnumResultField(tbName, "TIMED", []string{"YES", "NO"}, mysql.NotNullFlag, nil))
	return rfs
}

// CREATE TABLE if not exists performance_schema.setup_consumers (
// 		NAME			VARCHAR(64) NOT NULL,
// 		ENABLED			ENUM('YES','NO') NOT NULL);
func buildResultFieldsForSetupConsumers() (rfs []*field.ResultField) {
	tbName := tableSetupConsumers
	rfs = append(rfs, buildResultField(tbName, "NAME", mysql.TypeVarChar, 64, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildEnumResultField(tbName, "ENABLED", []string{'YES', 'NO'}, mysql.NotNullFlag, nil))
	return rfs
}

// CREATE TABLE if not exists performance_schema.setup_timers (
// 		NAME			VARCHAR(64) NOT NULL,
// 		TIMER_NAME		ENUM('NANOSECOND','MICROSECOND','MILLISECOND') NOT NULL);
func buildResultFieldsForSetupTimers() (rfs []*field.ResultField) {
	tbName := tableSetupTimers
	rfs = append(rfs, buildResultField(tbName, "NAME", mysql.TypeVarChar, 64, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildEnumResultField(tbName, "TIMER_NAME", []string{'NANOSECOND', 'MICROSECOND', 'MILLISECOND'}, mysql.NotNullFlag, nil))
	return rfs
}

// CREATE TABLE if not exists performance_schema.events_statements_current (
// 		THREAD_ID		BIGINT(20) UNSIGNED NOT NULL,
// 		EVENT_ID		BIGINT(20) UNSIGNED NOT NULL,
// 		END_EVENT_ID	BIGINT(20) UNSIGNED,
// 		EVENT_NAME		VARCHAR(128) NOT NULL,
// 		SOURCE			VARCHAR(64),
// 		TIMER_START		BIGINT(20) UNSIGNED,
// 		TIMER_END		BIGINT(20) UNSIGNED,
// 		TIMER_WAIT		BIGINT(20) UNSIGNED,
// 		LOCK_TIME		BIGINT(20) UNSIGNED NOT NULL,
// 		SQL_TEXT		LONGTEXT,
// 		DIGEST			VARCHAR(32),
// 		DIGEST_TEXT		LONGTEXT,
// 		CURRENT_SCHEMA	VARCHAR(64),
// 		OBJECT_TYPE		VARCHAR(64),
// 		OBJECT_SCHEMA	VARCHAR(64),
// 		OBJECT_NAME		VARCHAR(64),
// 		OBJECT_INSTANCE_BEGIN	BIGINT(20) UNSIGNED,
// 		MYSQL_ERRNO		INT(11),
// 		RETURNED_SQLSTATE		VARCHAR(5),
// 		MESSAGE_TEXT	VARCHAR(128),
// 		ERRORS			BIGINT(20) UNSIGNED NOT NULL,
// 		WARNINGS		BIGINT(20) UNSIGNED NOT NULL,
// 		ROWS_AFFECTED	BIGINT(20) UNSIGNED NOT NULL,
// 		ROWS_SENT		BIGINT(20) UNSIGNED NOT NULL,
// 		ROWS_EXAMINED	BIGINT(20) UNSIGNED NOT NULL,
// 		CREATED_TMP_DISK_TABLES	BIGINT(20) UNSIGNED NOT NULL,
// 		CREATED_TMP_TABLES		BIGINT(20) UNSIGNED NOT NULL,
// 		SELECT_FULL_JOIN		BIGINT(20) UNSIGNED NOT NULL,
// 		SELECT_FULL_RANGE_JOIN	BIGINT(20) UNSIGNED NOT NULL,
// 		SELECT_RANGE	BIGINT(20) UNSIGNED NOT NULL,
// 		SELECT_RANGE_CHECK		BIGINT(20) UNSIGNED NOT NULL,
// 		SELECT_SCAN		BIGINT(20) UNSIGNED NOT NULL,
// 		SORT_MERGE_PASSES		BIGINT(20) UNSIGNED NOT NULL,
// 		SORT_RANGE		BIGINT(20) UNSIGNED NOT NULL,
// 		SORT_ROWS		BIGINT(20) UNSIGNED NOT NULL,
// 		SORT_SCAN		BIGINT(20) UNSIGNED NOT NULL,
// 		NO_INDEX_USED	BIGINT(20) UNSIGNED NOT NULL,
// 		NO_GOOD_INDEX_USED		BIGINT(20) UNSIGNED NOT NULL,
// 		NESTING_EVENT_ID		BIGINT(20) UNSIGNED,
// 		NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE'),
// 		NESTING_EVENT_LEVEL		INT(11));
func buildResultFieldsForStmtsCurrent() (rfs []*field.ResultField) {
	tbName := tableStmtsCurrent
	rfs = append(rfs, buildResultField(tbName, "THREAD_ID", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "EVENT_ID", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "END_EVENT_ID", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "EVENT_NAME", mysql.TypeVarChar, 128, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SOURCE", mysql.TypeLongLong, 64, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "TIMER_START", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "TIMER_END", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "TIMER_WAIT", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "LOCK_TIME", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SQL_TEXT", mysql.TypeLongBlob, -1, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "DIGEST", mysql.TypeVarChar, 32, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "DIGEST_TEXT", mysql.TypeLongBlob, -1, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "CURRENT_SCHEMA", mysql.TypeVarChar, 64, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "OBJECT_TYPE", mysql.TypeVarChar, 64, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "OBJECT_SCHEMA", mysql.TypeVarChar, 64, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "OBJECT_NAME", mysql.TypeVarChar, 64, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "OBJECT_INSTANCE_BEGIN", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "MYSQL_ERRNO", mysql.TypeLong, 11, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "RETURNED_SQLSTATE", mysql.TypeVarChar, 5, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "MESSAGE_TEXT", mysql.TypeVarChar, 128, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "ERRORS", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "WARNINGS", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "ROWS_AFFECTED", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "ROWS_SENT", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "ROWS_EXAMINED", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "CREATED_TMP_DISK_TABLES", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "CREATED_TMP_TABLES", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SELECT_FULL_JOIN", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SELECT_FULL_RANGE_JOIN", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SELECT_RANGE", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SELECT_RANGE_CHECK", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SELECT_SCAN", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SORT_MERGE_PASSES", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SORT_RANGE", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SORT_ROWS", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SORT_SCAN", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "NO_INDEX_USED", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "NO_GOOD_INDEX_USED", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "NESTING_EVENT_ID", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildEnumResultField(tbName, "NESTING_EVENT_TYPE", []string{'TRANSACTION', 'STATEMENT', 'STAGE'}, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "NESTING_EVENT_LEVEL", mysql.TypeLong, 11, 0, nil))
	return rfs
}

// CREATE TABLE if not exists performance_schema.events_statements_history (
// 		THREAD_ID		BIGINT(20) UNSIGNED NOT NULL,
// 		EVENT_ID		BIGINT(20) UNSIGNED NOT NULL,
// 		END_EVENT_ID	BIGINT(20) UNSIGNED,
// 		EVENT_NAME		VARCHAR(128) NOT NULL,
// 		SOURCE			VARCHAR(64),
// 		TIMER_START		BIGINT(20) UNSIGNED,
// 		TIMER_END		BIGINT(20) UNSIGNED,
// 		TIMER_WAIT		BIGINT(20) UNSIGNED,
// 		LOCK_TIME		BIGINT(20) UNSIGNED NOT NULL,
// 		SQL_TEXT		LONGTEXT,
// 		DIGEST			VARCHAR(32),
// 		DIGEST_TEXT		LONGTEXT,
// 		CURRENT_SCHEMA	VARCHAR(64),
// 		OBJECT_TYPE		VARCHAR(64),
// 		OBJECT_SCHEMA	VARCHAR(64),
// 		OBJECT_NAME		VARCHAR(64),
// 		OBJECT_INSTANCE_BEGIN	BIGINT(20) UNSIGNED,
// 		MYSQL_ERRNO		INT(11),
// 		RETURNED_SQLSTATE		VARCHAR(5),
// 		MESSAGE_TEXT	VARCHAR(128),
// 		ERRORS			BIGINT(20) UNSIGNED NOT NULL,
// 		WARNINGS		BIGINT(20) UNSIGNED NOT NULL,
// 		ROWS_AFFECTED	BIGINT(20) UNSIGNED NOT NULL,
// 		ROWS_SENT		BIGINT(20) UNSIGNED NOT NULL,
// 		ROWS_EXAMINED	BIGINT(20) UNSIGNED NOT NULL,
// 		CREATED_TMP_DISK_TABLES	BIGINT(20) UNSIGNED NOT NULL,
// 		CREATED_TMP_TABLES		BIGINT(20) UNSIGNED NOT NULL,
// 		SELECT_FULL_JOIN		BIGINT(20) UNSIGNED NOT NULL,
// 		SELECT_FULL_RANGE_JOIN	BIGINT(20) UNSIGNED NOT NULL,
// 		SELECT_RANGE	BIGINT(20) UNSIGNED NOT NULL,
// 		SELECT_RANGE_CHECK		BIGINT(20) UNSIGNED NOT NULL,
// 		SELECT_SCAN		BIGINT(20) UNSIGNED NOT NULL,
// 		SORT_MERGE_PASSES		BIGINT(20) UNSIGNED NOT NULL,
// 		SORT_RANGE		BIGINT(20) UNSIGNED NOT NULL,
// 		SORT_ROWS		BIGINT(20) UNSIGNED NOT NULL,
// 		SORT_SCAN		BIGINT(20) UNSIGNED NOT NULL,
// 		NO_INDEX_USED	BIGINT(20) UNSIGNED NOT NULL,
// 		NO_GOOD_INDEX_USED		BIGINT(20) UNSIGNED NOT NULL,
// 		NESTING_EVENT_ID		BIGINT(20) UNSIGNED,
// 		NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE'),
// 		NESTING_EVENT_LEVEL		INT(11));
func buildResultFieldsForStmtsHistory() (rfs []*field.ResultField) {
	tbName := tableStmtsHistory
	rfs = append(rfs, buildResultField(tbName, "THREAD_ID", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "EVENT_ID", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "END_EVENT_ID", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "EVENT_NAME", mysql.TypeVarChar, 128, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SOURCE", mysql.TypeLongLong, 64, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "TIMER_START", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "TIMER_END", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "TIMER_WAIT", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "LOCK_TIME", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SQL_TEXT", mysql.TypeLongBlob, -1, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "DIGEST", mysql.TypeVarChar, 32, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "DIGEST_TEXT", mysql.TypeLongBlob, -1, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "CURRENT_SCHEMA", mysql.TypeVarChar, 64, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "OBJECT_TYPE", mysql.TypeVarChar, 64, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "OBJECT_SCHEMA", mysql.TypeVarChar, 64, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "OBJECT_NAME", mysql.TypeVarChar, 64, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "OBJECT_INSTANCE_BEGIN", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "MYSQL_ERRNO", mysql.TypeLong, 11, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "RETURNED_SQLSTATE", mysql.TypeVarChar, 5, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "MESSAGE_TEXT", mysql.TypeVarChar, 128, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "ERRORS", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "WARNINGS", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "ROWS_AFFECTED", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "ROWS_SENT", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "ROWS_EXAMINED", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "CREATED_TMP_DISK_TABLES", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "CREATED_TMP_TABLES", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SELECT_FULL_JOIN", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SELECT_FULL_RANGE_JOIN", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SELECT_RANGE", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SELECT_RANGE_CHECK", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SELECT_SCAN", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SORT_MERGE_PASSES", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SORT_RANGE", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SORT_ROWS", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SORT_SCAN", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "NO_INDEX_USED", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "NO_GOOD_INDEX_USED", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "NESTING_EVENT_ID", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildEnumResultField(tbName, "NESTING_EVENT_TYPE", []string{'TRANSACTION', 'STATEMENT', 'STAGE'}, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "NESTING_EVENT_LEVEL", mysql.TypeLong, 11, 0, nil))
	return rfs
}

// CREATE TABLE if not exists performance_schema.events_statements_history_long (
// 		THREAD_ID		BIGINT(20) UNSIGNED NOT NULL,
// 		EVENT_ID		BIGINT(20) UNSIGNED NOT NULL,
// 		END_EVENT_ID	BIGINT(20) UNSIGNED,
// 		EVENT_NAME		VARCHAR(128) NOT NULL,
// 		SOURCE			VARCHAR(64),
// 		TIMER_START		BIGINT(20) UNSIGNED,
// 		TIMER_END		BIGINT(20) UNSIGNED,
// 		TIMER_WAIT		BIGINT(20) UNSIGNED,
// 		LOCK_TIME		BIGINT(20) UNSIGNED NOT NULL,
// 		SQL_TEXT		LONGTEXT,
// 		DIGEST			VARCHAR(32),
// 		DIGEST_TEXT		LONGTEXT,
// 		CURRENT_SCHEMA	VARCHAR(64),
// 		OBJECT_TYPE		VARCHAR(64),
// 		OBJECT_SCHEMA	VARCHAR(64),
// 		OBJECT_NAME		VARCHAR(64),
// 		OBJECT_INSTANCE_BEGIN	BIGINT(20) UNSIGNED,
// 		MYSQL_ERRNO		INT(11),
// 		RETURNED_SQLSTATE		VARCHAR(5),
// 		MESSAGE_TEXT	VARCHAR(128),
// 		ERRORS			BIGINT(20) UNSIGNED NOT NULL,
// 		WARNINGS		BIGINT(20) UNSIGNED NOT NULL,
// 		ROWS_AFFECTED	BIGINT(20) UNSIGNED NOT NULL,
// 		ROWS_SENT		BIGINT(20) UNSIGNED NOT NULL,
// 		ROWS_EXAMINED	BIGINT(20) UNSIGNED NOT NULL,
// 		CREATED_TMP_DISK_TABLES	BIGINT(20) UNSIGNED NOT NULL,
// 		CREATED_TMP_TABLES		BIGINT(20) UNSIGNED NOT NULL,
// 		SELECT_FULL_JOIN		BIGINT(20) UNSIGNED NOT NULL,
// 		SELECT_FULL_RANGE_JOIN	BIGINT(20) UNSIGNED NOT NULL,
// 		SELECT_RANGE	BIGINT(20) UNSIGNED NOT NULL,
// 		SELECT_RANGE_CHECK		BIGINT(20) UNSIGNED NOT NULL,
// 		SELECT_SCAN		BIGINT(20) UNSIGNED NOT NULL,
// 		SORT_MERGE_PASSES		BIGINT(20) UNSIGNED NOT NULL,
// 		SORT_RANGE		BIGINT(20) UNSIGNED NOT NULL,
// 		SORT_ROWS		BIGINT(20) UNSIGNED NOT NULL,
// 		SORT_SCAN		BIGINT(20) UNSIGNED NOT NULL,
// 		NO_INDEX_USED	BIGINT(20) UNSIGNED NOT NULL,
// 		NO_GOOD_INDEX_USED		BIGINT(20) UNSIGNED NOT NULL,
// 		NESTING_EVENT_ID		BIGINT(20) UNSIGNED,
// 		NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE'),
// 		NESTING_EVENT_LEVEL		INT(11));
func buildResultFieldsForStmtsHistoryLong() (rfs []*field.ResultField) {
	tbName := tableStmtsHistoryLong
	rfs = append(rfs, buildResultField(tbName, "THREAD_ID", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "EVENT_ID", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "END_EVENT_ID", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "EVENT_NAME", mysql.TypeVarChar, 128, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SOURCE", mysql.TypeLongLong, 64, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "TIMER_START", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "TIMER_END", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "TIMER_WAIT", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "LOCK_TIME", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SQL_TEXT", mysql.TypeLongBlob, -1, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "DIGEST", mysql.TypeVarChar, 32, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "DIGEST_TEXT", mysql.TypeLongBlob, -1, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "CURRENT_SCHEMA", mysql.TypeVarChar, 64, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "OBJECT_TYPE", mysql.TypeVarChar, 64, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "OBJECT_SCHEMA", mysql.TypeVarChar, 64, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "OBJECT_NAME", mysql.TypeVarChar, 64, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "OBJECT_INSTANCE_BEGIN", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "MYSQL_ERRNO", mysql.TypeLong, 11, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "RETURNED_SQLSTATE", mysql.TypeVarChar, 5, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "MESSAGE_TEXT", mysql.TypeVarChar, 128, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "ERRORS", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "WARNINGS", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "ROWS_AFFECTED", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "ROWS_SENT", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "ROWS_EXAMINED", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "CREATED_TMP_DISK_TABLES", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "CREATED_TMP_TABLES", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SELECT_FULL_JOIN", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SELECT_FULL_RANGE_JOIN", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SELECT_RANGE", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SELECT_RANGE_CHECK", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SELECT_SCAN", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SORT_MERGE_PASSES", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SORT_RANGE", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SORT_ROWS", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SORT_SCAN", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "NO_INDEX_USED", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "NO_GOOD_INDEX_USED", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "NESTING_EVENT_ID", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildEnumResultField(tbName, "NESTING_EVENT_TYPE", []string{'TRANSACTION', 'STATEMENT', 'STAGE'}, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "NESTING_EVENT_LEVEL", mysql.TypeLong, 11, 0, nil))
	return rfs
}

// CREATE TABLE if not exists performance_schema.prepared_statements_instances (
// 		OBJECT_INSTANCE_BEGIN	BIGINT(20) UNSIGNED NOT NULL,
// 		STATEMENT_ID	BIGINT(20) UNSIGNED NOT NULL,
// 		STATEMENT_NAME	VARCHAR(64),
// 		SQL_TEXT		LONGTEXT NOT NULL,
// 		OWNER_THREAD_ID	BIGINT(20) UNSIGNED NOT NULL,
// 		OWNER_EVENT_ID	BIGINT(20) UNSIGNED NOT NULL,
// 		OWNER_OBJECT_TYPE		ENUM('EVENT','FUNCTION','TABLE'),
// 		OWNER_OBJECT_SCHEMA		VARCHAR(64),
// 		OWNER_OBJECT_NAME		VARCHAR(64),
// 		TIMER_PREPARE	BIGINT(20) UNSIGNED NOT NULL,
// 		COUNT_REPREPARE	BIGINT(20) UNSIGNED NOT NULL,
// 		COUNT_EXECUTE	BIGINT(20) UNSIGNED NOT NULL,
// 		SUM_TIMER_EXECUTE		BIGINT(20) UNSIGNED NOT NULL,
// 		MIN_TIMER_EXECUTE		BIGINT(20) UNSIGNED NOT NULL,
// 		AVG_TIMER_EXECUTE		BIGINT(20) UNSIGNED NOT NULL,
// 		MAX_TIMER_EXECUTE		BIGINT(20) UNSIGNED NOT NULL,
// 		SUM_LOCK_TIME	BIGINT(20) UNSIGNED NOT NULL,
// 		SUM_ERRORS		BIGINT(20) UNSIGNED NOT NULL,
// 		SUM_WARNINGS	BIGINT(20) UNSIGNED NOT NULL,
// 		SUM_ROWS_AFFECTED		BIGINT(20) UNSIGNED NOT NULL,
// 		SUM_ROWS_SENT	BIGINT(20) UNSIGNED NOT NULL,
// 		SUM_ROWS_EXAMINED		BIGINT(20) UNSIGNED NOT NULL,
// 		SUM_CREATED_TMP_DISK_TABLES	BIGINT(20) UNSIGNED NOT NULL,
// 		SUM_CREATED_TMP_TABLES	BIGINT(20) UNSIGNED NOT NULL,
// 		SUM_SELECT_FULL_JOIN	BIGINT(20) UNSIGNED NOT NULL,
// 		SUM_SELECT_FULL_RANGE_JOIN	BIGINT(20) UNSIGNED NOT NULL,
// 		SUM_SELECT_RANGE		BIGINT(20) UNSIGNED NOT NULL,
// 		SUM_SELECT_RANGE_CHECK	BIGINT(20) UNSIGNED NOT NULL,
// 		SUM_SELECT_SCAN	BIGINT(20) UNSIGNED NOT NULL,
// 		SUM_SORT_MERGE_PASSES	BIGINT(20) UNSIGNED NOT NULL,
// 		SUM_SORT_RANGE	BIGINT(20) UNSIGNED NOT NULL,
// 		SUM_SORT_ROWS	BIGINT(20) UNSIGNED NOT NULL,
// 		SUM_SORT_SCAN	BIGINT(20) UNSIGNED NOT NULL,
// 		SUM_NO_INDEX_USED		BIGINT(20) UNSIGNED NOT NULL,
// 		SUM_NO_GOOD_INDEX_USED	BIGINT(20) UNSIGNED NOT NULL);
func buildResultFieldsForPreparedStmtsInstances() (rfs []*field.ResultField) {
	tbName := tablePrepStmtsInstances
	rfs = append(rfs, buildResultField(tbName, "OBJECT_INSTANCE_BEGIN", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "STATEMENT_ID", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "STATEMENT_NAME", mysql.TypeVarChar, 64, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "SQL_TEXT", mysql.TypeLongBlob, -1, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "OWNER_THREAD_ID", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "OWNER_EVENT_ID", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildEnumResultField(tbName, "ONWER_OBJECT_TYPE", []string{'EVENT', 'FUNCTION', 'TABLE'}, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "OWNER_OBJECT_SCHEMA", mysql.TypeVarChar, 64, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "OWNER_OBJECT_NAME", mysql.TypeVarChar, 64, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "TIMER_PREPARE", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "COUNT_REPREPARE", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "COUNT_EXECUTE", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SUM_TIMER_EXECUTE", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "MIN_TIMER_EXECUTE", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "AVG_TIMER_EXECUTE", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "MAX_TIMER_EXECUTE", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SUM_LOCK_TIME", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SUM_ERRORS", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SUM_WARNINGS", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SUM_ROWS_AFFECTED", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SUM_ROWS_SENT", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SUM_ROWS_EXAMINED", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SUM_CREATED_TMP_DISK_TABLES", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SUM_CREATED_TMP_TABLES", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SUM_SELECT_FULL_JOIN", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SUM_SELECT_FULL_RANGE_JOIN", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SUM_SELECT_RANGE", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SUM_SELECT_RANGE_CHECK", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SUM_SELECT_SCAN", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SUM_SORT_MERGE_PASSES", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SUM_SORT_RANGE", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SUM_SORT_ROWS", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SUM_SORT_SCAN", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SUM_NO_INDEX_USED", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SUM_NO_GOOD_INDEX_USED", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	return rfs
}

// CREATE TABLE if not exists performance_schema.events_transactions_current (
// 		THREAD_ID		BIGINT(20) UNSIGNED NOT NULL,
// 		EVENT_ID		BIGINT(20) UNSIGNED NOT NULL,
// 		END_EVENT_ID	BIGINT(20) UNSIGNED,
// 		EVENT_NAME		VARCHAR(128) NOT NULL,
// 		STATE			ENUM('ACTIVE','COMMITTED',"ROLLED BACK"),
// 		TRX_ID			BIGINT(20) UNSIGNED,
// 		GTID			VARCHAR(64),
// 		XID_FORMAT_ID	INT(11),
// 		XID_GTRID		VARCHAR(130),
// 		XID_BQUAL		VARCHAR(130),
// 		XA_STATE		VARCHAR(64),
// 		SOURCE			VARCHAR(64),
// 		TIMER_START		BIGINT(20) UNSIGNED,
// 		TIMER_END		BIGINT(20) UNSIGNED,
// 		TIMER_WAIT		BIGINT(20) UNSIGNED,
// 		ACCESS_MODE		ENUM('READ ONLY','READ WRITE'),
// 		ISOLATION_LEVEL	VARCHAR(64),
// 		AUTOCOMMIT		ENUM('YES','NO') NOT NULL,
// 		NUMBER_OF_SAVEPOINTS	BIGINT(20) UNSIGNED,
// 		NUMBER_OF_ROLLBACK_TO_SAVEPOINT	BIGINT(20) UNSIGNED,
// 		NUMBER_OF_RELEASE_SAVEPOINT		BIGINT(20) UNSIGNED,
// 		OBJECT_INSTANCE_BEGIN	BIGINT(20) UNSIGNED,
// 		NESTING_EVENT_ID		BIGINT(20) UNSIGNED,
// 		NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE'));
func buildResultFieldsForTransCurrent() (rfs []*field.ResultField) {
	tbName := tableTransCurrent
	rfs = append(rfs, buildResultField(tbName, "THREAD_ID", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "EVENT_ID", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "END_EVENT_ID", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "EVENT_NAME", mysql.TypeVarChar, 128, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildEnumResultField(tbName, "STATE", []string{'ACTIVE', 'COMMITTED', 'ROLLED BACK'}, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "TRX_ID", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "GTID", mysql.TypeVarChar, 64, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "XID_FORMAT_ID", mysql.TypeLong, 11, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "XID_GTRID", mysql.TypeVarChar, 130, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "XID_BQUAL", mysql.TypeVarChar, 130, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "XA_STATE", mysql.TypeVarChar, 64, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "SOURCE", mysql.TypeVarChar, 64, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "TIMER_START", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "TIMER_END", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "TIMER_WAIT", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildEnumResultField(tbName, "ACCESS_MODE", []string{'READ ONLY', 'READ WRITE'}, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "ISOLATION_LEVEL", mysql.TypeVarChar, 64, 0, nil))
	rfs = append(rfs, buildEnumResultField(tbName, "AUTOCOMMIT", []string{'YES', 'NO'}, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "NUMBER_OF_SAVEPOINTS", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "NUMBER_OF_ROLLBACK_TO_SAVEPOINT", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "NUMBER_OF_RELEASE_SAVEPOINT", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "OBJECT_INSTANCE_BEGIN", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "NESTING_EVENT_ID", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildEnumResultField(tbName, "NESTING_EVENT_TYPE", []string{'TRANSACTION', 'STATEMENT', "STAGE"}, 0, nil))
	return rfs
}

// CREATE TABLE if not exists performance_schema.events_transactions_history (
// 		THREAD_ID		BIGINT(20) UNSIGNED NOT NULL,
// 		EVENT_ID		BIGINT(20) UNSIGNED NOT NULL,
// 		END_EVENT_ID	BIGINT(20) UNSIGNED,
// 		EVENT_NAME		VARCHAR(128) NOT NULL,
// 		STATE			ENUM('ACTIVE','COMMITTED',"ROLLED BACK"),
// 		TRX_ID			BIGINT(20) UNSIGNED,
// 		GTID			VARCHAR(64),
// 		XID_FORMAT_ID	INT(11),
// 		XID_GTRID		VARCHAR(130),
// 		XID_BQUAL		VARCHAR(130),
// 		XA_STATE		VARCHAR(64),
// 		SOURCE			VARCHAR(64),
// 		TIMER_START		BIGINT(20) UNSIGNED,
// 		TIMER_END		BIGINT(20) UNSIGNED,
// 		TIMER_WAIT		BIGINT(20) UNSIGNED,
// 		ACCESS_MODE		ENUM('READ ONLY','READ WRITE'),
// 		ISOLATION_LEVEL	VARCHAR(64),
// 		AUTOCOMMIT		ENUM('YES','NO') NOT NULL,
// 		NUMBER_OF_SAVEPOINTS	BIGINT(20) UNSIGNED,
// 		NUMBER_OF_ROLLBACK_TO_SAVEPOINT	BIGINT(20) UNSIGNED,
// 		NUMBER_OF_RELEASE_SAVEPOINT		BIGINT(20) UNSIGNED,
// 		OBJECT_INSTANCE_BEGIN	BIGINT(20) UNSIGNED,
// 		NESTING_EVENT_ID		BIGINT(20) UNSIGNED,
// 		NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE'));
func buildResultFieldsForTransHistory() (rfs []*field.ResultField) {
	tbName := tableTransHistory
	rfs = append(rfs, buildResultField(tbName, "THREAD_ID", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "EVENT_ID", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "END_EVENT_ID", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "EVENT_NAME", mysql.TypeVarChar, 128, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildEnumResultField(tbName, "STATE", []string{'ACTIVE', 'COMMITTED', 'ROLLED BACK'}, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "TRX_ID", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "GTID", mysql.TypeVarChar, 64, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "XID_FORMAT_ID", mysql.TypeLong, 11, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "XID_GTRID", mysql.TypeVarChar, 130, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "XID_BQUAL", mysql.TypeVarChar, 130, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "XA_STATE", mysql.TypeVarChar, 64, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "SOURCE", mysql.TypeVarChar, 64, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "TIMER_START", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "TIMER_END", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "TIMER_WAIT", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildEnumResultField(tbName, "ACCESS_MODE", []string{'READ ONLY', 'READ WRITE'}, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "ISOLATION_LEVEL", mysql.TypeVarChar, 64, 0, nil))
	rfs = append(rfs, buildEnumResultField(tbName, "AUTOCOMMIT", []string{'YES', 'NO'}, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "NUMBER_OF_SAVEPOINTS", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "NUMBER_OF_ROLLBACK_TO_SAVEPOINT", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "NUMBER_OF_RELEASE_SAVEPOINT", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "OBJECT_INSTANCE_BEGIN", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "NESTING_EVENT_ID", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildEnumResultField(tbName, "NESTING_EVENT_TYPE", []string{'TRANSACTION', 'STATEMENT', "STAGE"}, 0, nil))
	return rfs
}

// CREATE TABLE if not exists performance_schema.events_transactions_history_long (
// 		THREAD_ID		BIGINT(20) UNSIGNED NOT NULL,
// 		EVENT_ID		BIGINT(20) UNSIGNED NOT NULL,
// 		END_EVENT_ID	BIGINT(20) UNSIGNED,
// 		EVENT_NAME		VARCHAR(128) NOT NULL,
// 		STATE			ENUM('ACTIVE','COMMITTED',"ROLLED BACK"),
// 		TRX_ID			BIGINT(20) UNSIGNED,
// 		GTID			VARCHAR(64),
// 		XID_FORMAT_ID	INT(11),
// 		XID_GTRID		VARCHAR(130),
// 		XID_BQUAL		VARCHAR(130),
// 		XA_STATE		VARCHAR(64),
// 		SOURCE			VARCHAR(64),
// 		TIMER_START		BIGINT(20) UNSIGNED,
// 		TIMER_END		BIGINT(20) UNSIGNED,
// 		TIMER_WAIT		BIGINT(20) UNSIGNED,
// 		ACCESS_MODE		ENUM('READ ONLY','READ WRITE'),
// 		ISOLATION_LEVEL	VARCHAR(64),
// 		AUTOCOMMIT		ENUM('YES','NO') NOT NULL,
// 		NUMBER_OF_SAVEPOINTS	BIGINT(20) UNSIGNED,
// 		NUMBER_OF_ROLLBACK_TO_SAVEPOINT	BIGINT(20) UNSIGNED,
// 		NUMBER_OF_RELEASE_SAVEPOINT		BIGINT(20) UNSIGNED,
// 		OBJECT_INSTANCE_BEGIN	BIGINT(20) UNSIGNED,
// 		NESTING_EVENT_ID		BIGINT(20) UNSIGNED,
// 		NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE'));
func buildResultFieldsForTransHistoryLong() (rfs []*field.ResultField) {
	tbName := tableTransHistoryLong
	rfs = append(rfs, buildResultField(tbName, "THREAD_ID", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "EVENT_ID", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "END_EVENT_ID", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "EVENT_NAME", mysql.TypeVarChar, 128, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildEnumResultField(tbName, "STATE", []string{'ACTIVE', 'COMMITTED', 'ROLLED BACK'}, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "TRX_ID", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "GTID", mysql.TypeVarChar, 64, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "XID_FORMAT_ID", mysql.TypeLong, 11, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "XID_GTRID", mysql.TypeVarChar, 130, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "XID_BQUAL", mysql.TypeVarChar, 130, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "XA_STATE", mysql.TypeVarChar, 64, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "SOURCE", mysql.TypeVarChar, 64, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "TIMER_START", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "TIMER_END", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "TIMER_WAIT", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildEnumResultField(tbName, "ACCESS_MODE", []string{'READ ONLY', 'READ WRITE'}, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "ISOLATION_LEVEL", mysql.TypeVarChar, 64, 0, nil))
	rfs = append(rfs, buildEnumResultField(tbName, "AUTOCOMMIT", []string{'YES', 'NO'}, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "NUMBER_OF_SAVEPOINTS", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "NUMBER_OF_ROLLBACK_TO_SAVEPOINT", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "NUMBER_OF_RELEASE_SAVEPOINT", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "OBJECT_INSTANCE_BEGIN", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "NESTING_EVENT_ID", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildEnumResultField(tbName, "NESTING_EVENT_TYPE", []string{'TRANSACTION', 'STATEMENT', "STAGE"}, 0, nil))
	return rfs
}

// CREATE TABLE if not exists performance_schema.events_stages_current (
// 		THREAD_ID		BIGINT(20) UNSIGNED NOT NULL,
// 		EVENT_ID		BIGINT(20) UNSIGNED NOT NULL,
// 		END_EVENT_ID	BIGINT(20) UNSIGNED,
// 		EVENT_NAME		VARCHAR(128) NOT NULL,
// 		SOURCE			VARCHAR(64),
// 		TIMER_START		BIGINT(20) UNSIGNED,
// 		TIMER_END		BIGINT(20) UNSIGNED,
// 		TIMER_WAIT		BIGINT(20) UNSIGNED,
// 		WORK_COMPLETED	BIGINT(20) UNSIGNED,
// 		WORK_ESTIMATED	BIGINT(20) UNSIGNED,
// 		NESTING_EVENT_ID		BIGINT(20) UNSIGNED,
// 		NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE'));
func buildResultFieldsForStagesCurrent() (rfs []*field.ResultField) {
	tbName := tableStagesCurrent
	rfs = append(rfs, buildResultField(tbName, "THREAD_ID", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "EVENT_ID", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "END_EVENT_ID", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "EVENT_NAME", mysql.TypeVarChar, 128, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SOURCE", mysql.TypeVarChar, 64, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "TIMER_START", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "TIMER_END", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "TIMER_WAIT", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "WORK_COMPLETED", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "WORK_ESTIMATED", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "NESTING_EVENT_ID", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildEnumResultField(tbName, "NESTING_EVENT_TYPE", []string{'TRANSACTION', 'STATEMENT', "STAGE"}, 0, nil))
	return rfs
}

// CREATE TABLE if not exists performance_schema.events_stages_history (
// 		THREAD_ID		BIGINT(20) UNSIGNED NOT NULL,
// 		EVENT_ID		BIGINT(20) UNSIGNED NOT NULL,
// 		END_EVENT_ID	BIGINT(20) UNSIGNED,
// 		EVENT_NAME		VARCHAR(128) NOT NULL,
// 		SOURCE			VARCHAR(64),
// 		TIMER_START		BIGINT(20) UNSIGNED,
// 		TIMER_END		BIGINT(20) UNSIGNED,
// 		TIMER_WAIT		BIGINT(20) UNSIGNED,
// 		WORK_COMPLETED	BIGINT(20) UNSIGNED,
// 		WORK_ESTIMATED	BIGINT(20) UNSIGNED,
// 		NESTING_EVENT_ID		BIGINT(20) UNSIGNED,
// 		NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE'));
func buildResultFieldsForStagesHistory() (rfs []*field.ResultField) {
	tbName := tableStagesHistory
	rfs = append(rfs, buildResultField(tbName, "THREAD_ID", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "EVENT_ID", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "END_EVENT_ID", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "EVENT_NAME", mysql.TypeVarChar, 128, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SOURCE", mysql.TypeVarChar, 64, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "TIMER_START", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "TIMER_END", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "TIMER_WAIT", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "WORK_COMPLETED", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "WORK_ESTIMATED", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "NESTING_EVENT_ID", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildEnumResultField(tbName, "NESTING_EVENT_TYPE", []string{'TRANSACTION', 'STATEMENT', "STAGE"}, 0, nil))
	return rfs
}

// CREATE TABLE if not exists performance_schema.events_stages_history_long (
// 		THREAD_ID		BIGINT(20) UNSIGNED NOT NULL,
// 		EVENT_ID		BIGINT(20) UNSIGNED NOT NULL,
// 		END_EVENT_ID	BIGINT(20) UNSIGNED,
// 		EVENT_NAME		VARCHAR(128) NOT NULL,
// 		SOURCE			VARCHAR(64),
// 		TIMER_START		BIGINT(20) UNSIGNED,
// 		TIMER_END		BIGINT(20) UNSIGNED,
// 		TIMER_WAIT		BIGINT(20) UNSIGNED,
// 		WORK_COMPLETED	BIGINT(20) UNSIGNED,
// 		WORK_ESTIMATED	BIGINT(20) UNSIGNED,
// 		NESTING_EVENT_ID		BIGINT(20) UNSIGNED,
// 		NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE'));
func buildResultFieldsForStagesHistoryLong() (rfs []*field.ResultField) {
	tbName := tableStagesHistoryLong
	rfs = append(rfs, buildResultField(tbName, "THREAD_ID", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "EVENT_ID", mysql.TypeLongLong, 20, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "END_EVENT_ID", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "EVENT_NAME", mysql.TypeVarChar, 128, mysql.NotNullFlag, nil))
	rfs = append(rfs, buildResultField(tbName, "SOURCE", mysql.TypeVarChar, 64, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "TIMER_START", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "TIMER_END", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "TIMER_WAIT", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "WORK_COMPLETED", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "WORK_ESTIMATED", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildResultField(tbName, "NESTING_EVENT_ID", mysql.TypeLongLong, 20, 0, nil))
	rfs = append(rfs, buildEnumResultField(tbName, "NESTING_EVENT_TYPE", []string{'TRANSACTION', 'STATEMENT', "STAGE"}, 0, nil))
	return rfs
}

func buildSetupActorsRecords() (records [][]interface{}) {
	records = append(records,
		[]interface{}{"%", "%", "%", "Yes", "Yes"},
	)
	return records
}

func buildSetupObjectsRecords() (records [][]interface{}) {
	records = append(records,
		[]interface{}{"EVENT", "mysql", "%", "NO", "NO"},
		[]interface{}{"EVENT", "performance_schema", "%", "NO", "NO"},
		[]interface{}{"EVENT", "information_schema", "%", "NO", "NO"},
		[]interface{}{"EVENT", "%", "%", "YES", "YES"},
		[]interface{}{"FUNCTION", "mysql", "%", "NO", "NO"},
		[]interface{}{"FUNCTION", "performance_schema", "%", "NO", "NO"},
		[]interface{}{"FUNCTION", "information_schema", "%", "NO", "NO"},
		[]interface{}{"FUNCTION", "%", "%", "YES", "YES"},
		[]interface{}{"TABLE", "mysql", "%", "NO", "NO"},
		[]interface{}{"TABLE", "performance_schema", "%", "NO", "NO"},
		[]interface{}{"TABLE", "information_schema", "%", "NO", "NO"},
		[]interface{}{"TABLE", "%", "%", "YES", "YES"},
	)
	return records
}

func buildSetupInstrsRecords() (records [][]interface{}) {
	// TODO: add instrumentation points later
	return records
}

func buildSetupConsumersRecords() (records [][]interface{}) {
	records = append(records,
		[]interface{}{"events_stages_current", "NO"},
		[]interface{}{"events_stages_history", "NO"},
		[]interface{}{"events_stages_history_long", "NO"},
		[]interface{}{"events_statements_current", "YES"},
		[]interface{}{"events_statements_history", "YES"},
		[]interface{}{"events_statements_history_long", "NO"},
		[]interface{}{"events_transactions_current", "YES"},
		[]interface{}{"events_transactions_history", "YES"},
		[]interface{}{"events_transactions_history_long", "YES"},
		[]interface{}{"global_instrumentation", "YES"},
		[]interface{}{"thread_instrumentation", "YES"},
		[]interface{}{"statements_digest", "YES"},
	)
	return records
}

func buildSetupTimersRecords() (records [][]interface{}) {
	records = append(records,
		[]interface{}{"stage", "NANOSECOND"},
		[]interface{}{"statement", "NANOSECOND"},
		[]interface{}{"transaction", "NANOSECOND"},
	)
	return records
}

// Explain implements plan.Plan Explain interface.
func (isp *PerfSchemaPlan) Explain(w format.Formatter) {}

// Filter implements plan.Plan Filter interface.
func (isp *PerfSchemaPlan) Filter(ctx context.Context, expr expression.Expression) (p plan.Plan, filtered bool, err error) {
	return isp, false, nil
}

// GetFields implements plan.Plan GetFields interface, simulates MySQL's output.
func (isp *PerfSchemaPlan) GetFields() []*field.ResultField {
	switch isp.TableName {
	case tableSetupActors:
		return setupActorsFields
	case tableSetupObjects:
		return setupObjectsFields
	case tableSetupInstrs:
		return setupInstrsFields
	case tableSetupConsumers:
		return setupCosumersFields
	case tableSetupTimers:
		return setupTimersFields
	case tableStmtsCurrent:
		return stmtsCurrentFields
	case tableStmtsHistory:
		return stmtsHistoryFields
	case tableStmtsHistoryLong:
		return stmtsHistoryLongFields
	case tablePrepStmtsInstances:
		return preparedStmtsInstancesFields
	case tableTransCurrent:
		return transCurrentFields
	case tableTransHistory:
		return transHistoryFields
	case tableTransHistoryLong:
		return transHistoryLongFields
	case tableStagesCurrent:
		return stagesCurrentFields
	case tableStagesHistory:
		return stagesHistoryFields
	case tableStagesHistoryLong:
		return stagesHistoryLongFields
	}
	return nil
}

func buildResultField(tableName, name string, tp byte, size int, flag uint, def interface{}) *field.ResultField {
	mCharset := charset.CharsetBin
	mCollation := charset.CharsetBin
	if tp == mysql.TypeString || tp == mysql.TypeVarchar || tp == mysql.TypeBlob || tp == mysql.TypeLongBlob {
		mCharset = mysql.DefaultCharset
		mCollation = mysql.DefaultCollationName
	}
	// TODO: does TypeLongBlob need size?
	fieldType := types.FieldType{
		Charset: mCharset,
		Collate: mCollation,
		Tp:      tp,
		Flen:    size,
		Flag:    uint(flag),
	}
	colInfo := model.ColumnInfo{
		Name:         model.NewCIStr(name),
		FieldType:    fieldType,
		DefaultValue: def,
	}
	field := &field.ResultField{
		Col:       column.Col{ColumnInfo: colInfo},
		DBName:    infoschema.Name,
		TableName: tableName,
		Name:      colInfo.Name.O,
	}
	return field
}

func buildEnumResultField(tableName, name string, elems []string, flag uint, def interface{}) {
	mCharset := charset.CharsetBin
	mCollation := charset.CharsetBin
	fieldType := types.FieldType{
		Charset: mCharset,
		Collate: mCollation,
		Tp:      mysql.TypeEnum,
		Flag:    uint(flag),
		Elems:   elems,
	}
	colInfo := model.ColumnInfo{
		Name:         model.NewCIStr(name),
		FieldType:    fieldType,
		DefaultValue: def,
	}
	field := &field.ResultField{
		Col:       column.Col{ColumnInfo: colInfo},
		DBName:    infoschema.Name,
		TableName: tableName,
		Name:      colInfo.Name.O,
	}
	return field
}

// Next implements plan.Plan Next interface.
func (isp *PerfSchemaPlan) Next(ctx context.Context) (row *plan.Row, err error) {
	if isp.rows == nil {
		isp.fetchAll(ctx)
	}
	if isp.cursor == len(isp.rows) {
		return
	}
	row = isp.rows[isp.cursor]
	isp.cursor++
	return
}

func (isp *PerfSchemaPlan) fetchAll(ctx context.Context) {
	// TODO: need support INSERT/DELETE/UPDATE operations
	switch isp.TableName {
	case tableSetupActors:
		isp.fetchSetupActors()
	case tableSetupObjects:
		isp.fetchSetupObjects()
	case tableSetupInstrs:
		isp.fetchSetupInstrs()
	case tableSetupConsumers:
		isp.fetchSetupConsumers()
	case tableSetupTimers:
		isp.fetchSetupTimers()
	}
}

func (isp *PerfSchemaPlan) fetchSetupActors() {
	for _, record := range setupActorsRecords {
		isp.rows = append(isp.rows, &plan.Row{Data: record})
	}
}

func (isp *PerfSchemaPlan) fetchSetupObjects() {
	for _, record := range setupObjectsRecords {
		isp.rows = append(isp.rows, &plan.Row{Data: record})
	}
}

func (isp *PerfSchemaPlan) fetchSetupInstrs() {
	for _, record := range setupInstrsRecords {
		isp.rows = append(isp.rows, &plan.Row{Data: record})
	}
}

func (isp *PerfSchemaPlan) fetchSetupConsumers() {
	for _, record := range setupConsumersRecords {
		isp.rows = append(isp.rows, &plan.Row{Data: record})
	}
}

func (isp *PerfSchemaPlan) fetchSetupTimers() {
	for _, record := range setupTimersRecords {
		isp.rows = append(isp.rows, &plan.Row{Data: record})
	}
}

// Close implements plan.Plan Close interface.
func (isp *InfoSchemaPlan) Close() error {
	isp.rows = nil
	isp.cursor = 0
	return nil
}

func init() {
	setupActorsFields = buildResultFieldsForSetupActors()
	setupObjectsFields = buildResultFieldsForSetupObjects()
	setupInstrsFields = buildResultFieldsForSetupInstrs()
	setupConsumersFields = buildResultFieldsForSetupConsumers()
	setupTimersFields = buildResultFieldsForSetupTimers()
	stmtsCurrentFields = buildResultFieldsForStmtsCurrent()
	stmtsHistoryFields = buildResultFieldsForStmtsHistory()
	stmtsHistoryLongFields = buildResultFieldsForStmtsHistoryLong()
	preparedStmtsInstancesFields = buildResultFieldsForPreparedStmtsInstances()
	transCurrentFields = buildResultFieldsForTransCurrent()
	transHistoryFields = buildResultFieldsForTransHistory()
	transHistoryLongFields = buildResultFieldsForTransHistoryLong()
	stagesCurrentFields = buildResultFieldsForStagesCurrent()
	stagesHistoryFields = buildResultFieldsForStagesHistory()
	stagesHistoryLongFields = buildResultFieldsForStagesHistoryLong()

	setupActorsRecords = buildSetupActorsRecords()
	setupObjectsRecords = buildSetupObjectsRecords()
	setupInstrsRecords = buildSetupInstrsRecords()
	setupConsumersRecords = buildSetupConsumersRecords()
	setupTimersRecords = buildSetupTimersRecords()
}
