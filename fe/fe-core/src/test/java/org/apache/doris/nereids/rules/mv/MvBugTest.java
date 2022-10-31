// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.rules.mv;

import org.apache.doris.common.FeConstants;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * for https://github.com/apache/doris/issues/13560
 */
public class MvBugTest extends TestWithFeService {

    @Override
    protected void beforeCreatingConnectContext() throws Exception {
        FeConstants.default_scheduler_interval_millisecond = 10;
        FeConstants.runningUnitTest = true;
    }

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        useDatabase("test");

        createTable("CREATE TABLE test_join_left_table (\n"
                + "  `dt` BIGINT NULL COMMENT \"日期\",\n"
                + "  `key1` BIGINT NULL COMMENT \"key1\",\n"
                + "  `key2` BIGINT NULL COMMENT \"key2\",\n"
                + "  `value1` INT NULL COMMENT \"value1\"\n"
                + ") ENGINE = OLAP\n"
                + "DUPLICATE KEY(`dt`, `key1`, `key2`)\n"
                + "COMMENT \"测试join左表\"\n"
                + "PARTITION BY RANGE(`dt`)\n"
                + "(\n"
                + "  PARTITION p20221022 VALUES  [(\"20221021\"),(\"20221022\"))\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(`key1`) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"storage_format\" = \"v2\");\n");

        createTable("CREATE TABLE test_join_right_table_has_mv (\n"
                + "  `dt` INT NULL COMMENT \"dt\",\n"
                + "  `key1` BIGINT NULL COMMENT \"key1\",\n"
                + "  `key3` BIGINT NULL COMMENT \"key3\",\n"
                + "  `value2` INT NULL COMMENT \"value2\"\n"
                + ") ENGINE = OLAP\n"
                + "DUPLICATE KEY(`dt`, `key1`, `key3`)\n"
                + "COMMENT \"测试join右表存在物化\"\n"
                + "PARTITION BY RANGE(`dt`)\n"
                + "(\n"
                + "  PARTITION p20221022 VALUES  [(\"20221021\"),(\"20221022\"))\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(`key3`) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"storage_format\" = \"v2\");\n");

        createMv("create materialized view test_join_right_table_has_mv_dt_key1\n"
                + "as select dt, key1, sum(value2)\n"
                + "from test_join_right_table_has_mv\n"
                + "group by dt, key1; \n");
    }

    @Test
    public void test() throws Exception {
        String explain = getSQLPlanOrErrorMsg("select t1.dt,\n"
                + "       t1.key1,\n"
                + "       t1.key2,\n"
                + "       sum(t1.value1) as value1,\n"
                + "       sum(t2.value2) as value2\n"
                + "  from test_join_left_table t1\n"
                + "  left join test_join_right_table_has_mv t2\n"
                + "    on t1.dt = t2.dt\n"
                + "   and t1.key1 = t2.key1\n"
                + " group by t1.dt,\n"
                + "          t1.key1,\n"
                + "          t1.key2\n"
                + " order by t1.dt,\n"
                + "          t1.key1,\n"
                + "          t1.key2; ");

        Assertions.assertFalse(explain.contains("test_join_right_table_has_mv_dt_key1"));
    }
}
