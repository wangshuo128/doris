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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import org.junit.jupiter.api.Test;

import java.util.Optional;

class LimitPushDownTest implements PatternMatchSupported {

    private Plan plan = new LogicalLimit<>(10, 0,
            new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                    Optional.of(new EqualTo(new UnboundSlot("sid"), new UnboundSlot("id"))),
                    new LogicalOlapScan(PlanConstructor.score),
                    new LogicalOlapScan(PlanConstructor.student)
            )
    );

    @Test
    public void test() {
        PlanChecker.from(MemoTestUtils.createConnectContext())
                .analyze(plan)
                .applyTopDown(new LimitPushDown())
                .matches(
                        logicalJoin(
                                logicalLimit(
                                        logicalOlapScan()
                                ),
                                logicalOlapScan()
                        )
                );
    }

}
