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

package org.apache.doris.sql.optimizer;

import java.util.List;

import com.google.common.collect.Lists;
import org.apache.doris.sql.plan.logical.LogicalPlan;

public class Optimizer {

    public List<OptimizerRule> rules;

    public Optimizer(List<OptimizerRule> rules) {
        this.rules = rules;
    }

    public LogicalPlan optimize(LogicalPlan plan) {
        LogicalPlan root = plan;
        for (OptimizerRule rule : rules) {
            root = rule.apply(root);
            System.out.println("--- after running optimizer rule <" +
                                   rule.getClass().getSimpleName() +
                                   ">, plan is\n" + root.treeString() + "\n");
        }
        return root;
    }

    public static Optimizer create(OptimizerRule... rules) {
        List<OptimizerRule> ruleSet = Lists.newArrayList(rules);
        return new Optimizer(ruleSet);
    }
}
