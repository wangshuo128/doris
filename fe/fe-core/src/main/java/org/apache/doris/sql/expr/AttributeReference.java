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

import com.google.common.base.Objects;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.sql.rule.ExprVisitor;

import static org.apache.doris.sql.expr.ExprIdUtil.nextExprId;

public class AttributeReference extends Attribute {
    private final String name;
    private final Type type;
    private final Boolean nullable;
    private final Optional<String> qualifier;
    private final long exprId;

    public AttributeReference(String name, Type type, Boolean nullable, Optional<String> qualifier, long exprId) {
        this.name = name;
        this.type = type;
        this.nullable = nullable;
        this.qualifier = qualifier;
        this.exprId = exprId;
    }

    public AttributeReference(String name, Type type, Boolean nullable, Optional<String> qualifier) {
        this.name = name;
        this.type = type;
        this.nullable = nullable;
        this.qualifier = qualifier;
        this.exprId = nextExprId();
    }

    public AttributeReference(String name, Type type, Boolean nullable) {
        this(name, type, nullable, Optional.empty());
    }

    @Override
    public AttributeReference withNewQualifier(Optional<String> qualifier) {
        return new AttributeReference(name, type, nullable, qualifier, exprId);
    }


    public static AttributeReference fromColumn(Column column, String tableName) {
        return new AttributeReference(column.getName(), column.getType(), column.isAllowNull(), Optional.of(tableName));
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public long getExprId() {
        return exprId;
    }

    public Optional<String> getQualifier() {
        return qualifier;
    }

    @Override
    public <R, C> R accept(ExprVisitor<R, C> visitor, C context) {
        return visitor.visitAttributeReference(this, context);
    }

    @Override
    public String toString() {
        String uniqueName = name + "#" + exprId;
        return qualifier.map(q -> q + "." + uniqueName).orElse(uniqueName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AttributeReference)) return false;
        AttributeReference that = (AttributeReference) o;
        return getExprId() == that.getExprId();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getExprId());
    }

    @Override
    public AttributeSet getReferences() {
        return new AttributeSet(this);
    }

    @Override
    public Attribute toAttribute() {
        return this;
    }

    @Override
    public Type getType() {
        return type;
    }

    @Override
    public boolean isNullable() {
        return nullable;
    }
}
