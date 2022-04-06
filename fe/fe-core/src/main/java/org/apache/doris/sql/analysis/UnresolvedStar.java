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

package org.apache.doris.sql.analysis;

import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.apache.doris.sql.expr.Expression;
import org.apache.doris.sql.rule.ExprVisitor;

public class UnresolvedStar extends Expression {
    private Optional<List<String>> target;

    public UnresolvedStar(Optional<List<String>> target) {
        this.target = target;
    }

    public <R, C> R accept(ExprVisitor<R, C> visitor, C context) {
        return visitor.visitUnresolvedStar(this, context);
    }

    @Override
    public String toString() {
        return target.map(strings -> StringUtils.join(strings, ".") + "*").orElse("*");
    }
}
