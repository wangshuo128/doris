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

import java.util.Optional;

import org.apache.doris.sql.expr.Attribute;
import org.apache.doris.sql.expr.Expression;
import org.apache.doris.sql.expr.NamedExpression;

public class UnresolvedAlias extends NamedExpression {

    public final Expression child;

    public UnresolvedAlias(Expression child) {
        this.child = child;
    }

    @Override
    public Optional<String> getQualifier() {
        return Optional.empty();
    }

    @Override
    public String getName() {
        throw new RuntimeException("not supported");
    }

    @Override
    public long getExprId() {
        throw new RuntimeException("not supported");
    }

    @Override
    public Attribute toAttribute() {
        throw new RuntimeException("not supported");
    }

    @Override
    public String getQualifiedName() {
        throw new RuntimeException("not supported");
    }
}
