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

package org.apache.doris.sql.expr;

import java.util.Optional;

import org.apache.doris.sql.rule.ExprVisitor;

import static org.apache.doris.sql.expr.ExprIdUtil.nextExprId;

public class Alias extends NamedExpression {
    public final Expression child;
    public final String name;
    public final long exprId;

    @Override
    public Optional<String> getQualifier() {
        return Optional.empty();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public long getExprId() {
        return exprId;
    }

    @Override
    public String getQualifiedName() {
        return name;
    }

    @Override
    public Attribute toAttribute() {
        // todo: handle not analyzed
        return new AttributeReference(name, child.getType(), child.isNullable());
    }

    public Alias(Expression child, String name) {
        this.child = child;
        this.name = name;
        this.exprId = nextExprId();
    }

    @Override
    public <R, C> R accept(ExprVisitor<R, C> visitor, C context) {
        return visitor.visitAlias(this, context);
    }

    @Override
    public String toString() {
        return new StringBuilder(child.toString()).append(" AS ").append(name).toString();
    }
}
