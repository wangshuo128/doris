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

package org.apache.doris.planner;


public abstract class PlanNodeVisitor<R, C> {

    public abstract R visit(PlanNode plan, C context);

    public R visitOlapScanNode(OlapScanNode scan, C context) {
        return visit(scan, context);
    }

    public R visitHashJoinNode(HashJoinNode join, C context) {
        return visit(join, context);
    }

    public R visitExchangeNode(ExchangeNode exchange, C context) {
        return visit(exchange, context);
    }


    // todo: support more node types
}
