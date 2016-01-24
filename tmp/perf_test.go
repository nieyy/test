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

package plans_test

import (
	"database/sql"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
)

type testPerfSchemaSuit struct {
	vars map[string]interface{}
}

var _ = Suite(&testPerfSchemaSuit{
	vars: make(map[string]interface{}),
})

func mustBegin(c *C, currDB *sql.DB) *sql.Tx {
	tx, err := currDB.Begin()
	c.Assert(err, IsNil)
	return tx
}

func mustCommit(c *C, tx *sql.Tx) {
	err := tx.Commit()
	c.Assert(err, IsNil)
}

func mustExecuteSql(c *C, tx *sql.Tx, sql string) sql.Result {
	r, err := tx.Exec(sql)
	c.Assert(err, IsNil)
	return r
}

func mustQuery(c *C, currDB *sql.DB, s string) int {
	tx := mustBegin(c, currDB)
	r, err := tx.Query(s)
	c.Assert(err, IsNil)
	cols, _ := r.Columns()
	l := len(cols)
	c.Assert(l, Greater, 0)
	cnt := 0
	res := make([]interface{}, l)
	for i := 0; i < l; i++ {
		res[i] = &sql.RawBytes{}
	}
	for r.Next() {
		err := r.Scan(res...)
		c.Assert(err, IsNil)
		cnt++
	}
	c.Assert(r.Err(), IsNil)
	r.Close()
	mustCommit(c, tx)
	return cnt
}

func mustFailQuery(c *C, currDB *sql.DB, s string) {
	rows, err := currDB.Query(s)
	c.Assert(err, IsNil)
	rows.Next()
	c.Assert(rows.Err(), NotNil)
	rows.Close()
}

func mustExec(c *C, currDB *sql.DB, sql string) sql.Result {
	tx := mustBegin(c, currDB)
	r := mustExecuteSql(c, tx, sql)
	mustCommit(c, tx)
	return r
}

func (p *testPerfSchemaSuit) TestPerfSchema(c *C) {
	testDB, err := sql.Open(tidb.DriverName, tidb.EngineGoLevelDBMemory+"/test/test")
	c.Assert(err, IsNil)
	cnt = mustQuery(c, testDB, "select * from information_schema.setup_actors")
	c.Assert(cnt, Equals, 1)
	cnt = mustQuery(c, testDB, "select * from information_schema.setup_objects")
	c.Assert(cnt, Equals, 12)
	cnt = mustQuery(c, testDB, "select * from information_schema.setup_instrs")
	// So far, there has no instrumentation points yet
	c.Assert(cnt, Equals, 0)
	cnt = mustQuery(c, testDB, "select * from information_schema.setup_consumers")
	c.Assert(cnt, Equals, 12)
	cnt = mustQuery(c, testDB, "select * from information_schema.setup_timers")
	c.Assert(cnt, Equals, 3)
	cnt := mustQuery(c, testDB, "select * from information_schema.events_statements_current")
	c.Assert(cnt, Equals, 0)
	cnt := mustQuery(c, testDB, "select * from information_schema.events_statements_history")
	c.Assert(cnt, Equals, 0)
	cnt := mustQuery(c, testDB, "select * from information_schema.events_statements_history_long")
	c.Assert(cnt, Equals, 0)
	cnt := mustQuery(c, testDB, "select * from information_schema.prepared_statements_instances")
	c.Assert(cnt, Equals, 0)
	cnt := mustQuery(c, testDB, "select * from information_schema.events_transactions_current")
	c.Assert(cnt, Equals, 0)
	cnt := mustQuery(c, testDB, "select * from information_schema.events_transactions_history")
	c.Assert(cnt, Equals, 0)
	cnt := mustQuery(c, testDB, "select * from information_schema.events_transactions_history_long")
	c.Assert(cnt, Equals, 0)
	cnt := mustQuery(c, testDB, "select * from information_schema.events_stages_current")
	c.Assert(cnt, Equals, 0)
	cnt := mustQuery(c, testDB, "select * from information_schema.events_stages_history")
	c.Assert(cnt, Equals, 0)
	cnt := mustQuery(c, testDB, "select * from information_schema.events_stages_history_long")
	c.Assert(cnt, Equals, 0)
}
