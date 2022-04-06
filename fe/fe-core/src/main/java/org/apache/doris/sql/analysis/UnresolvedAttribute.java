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
import org.apache.doris.sql.rule.ExprVisitor;

public class UnresolvedAttribute extends Attribute {

    private Optional<String> base;
    private String name;

    public Optional<String> getBase() {
        return base;
    }

    public String getName() {
        return name;
    }

    public UnresolvedAttribute(String name) {
        this.name = name;
        base = Optional.empty();
    }

    public UnresolvedAttribute(String base, String name) {
        this.base = Optional.of(base);
        this.name = name;
    }

    public Optional<String> getQualifier() {
        return base;
    }

    @Override
    public Attribute withNewQualifier(Optional<String> qualifier) {
        throw new RuntimeException("not supported");
    }

    @Override
    public long getExprId() {
        throw new RuntimeException("Not supported in UnresolvedAttribute");
    }

    @Override
    public Attribute toAttribute() {
        throw new RuntimeException("not supported");
    }

    public <R, C> R accept(ExprVisitor<R, C> visitor, C context) {
        return visitor.visitUnresolvedAttribute(this, context);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (base.isPresent()) {
            sb.append(base.get()).append(".");
        }
        sb.append(name);
        return sb.toString();
    }
}
