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
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

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

        createTable("create table sales_records\n"
                + "(record_id int, \n"
                + "seller_id int, \n"
                + "store_id int, \n"
                + "sale_date date, \n"
                + "sale_amt bigint) \n"
                + "distributed by hash(record_id) \n"
                + "properties(\"replication_num\" = \"1\");");

        createMv("create materialized view store_amt as select store_id, sum(sale_amt) "
                + "from sales_records group by store_id");
    }


    @Test
    public void test() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("SELECT store_id, sum(sale_amt) FROM sales_records GROUP BY store_id", planner -> {
                    List<ScanNode> scans = planner.getScanNodes();
                    Assertions.assertEquals(1, scans.size());
                    ScanNode scanNode = scans.get(0);
                    Assertions.assertTrue(scanNode instanceof OlapScanNode);
                    System.out.println("index name: " + ((OlapScanNode) scanNode).getSelectedIndexName());
                });

    }
}
