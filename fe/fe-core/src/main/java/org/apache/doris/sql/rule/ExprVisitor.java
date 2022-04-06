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

package org.apache.doris.sql.rule;

import org.apache.doris.sql.analysis.UnresolvedAttribute;
import org.apache.doris.sql.analysis.UnresolvedStar;
import org.apache.doris.sql.expr.Alias;
import org.apache.doris.sql.expr.AttributeReference;
import org.apache.doris.sql.expr.BinaryPredicate;
import org.apache.doris.sql.expr.Expression;
import org.apache.doris.sql.expr.InPredicate;
import org.apache.doris.sql.expr.IntLiteral;
import org.apache.doris.sql.expr.SubQuery;

public abstract class ExprVisitor<R, C> {
    public abstract R visit(Expression expr, C context);

    public R visitUnresolvedAttribute(UnresolvedAttribute expr, C context) {
        return visit(expr, context);
    }

    public R visitUnresolvedStar(UnresolvedStar expr, C context) {
        return visit(expr, context);
    }

    public R visitAlias(Alias expr, C context) {
        return visit(expr, context);
    }

    public R visitAttributeReference(AttributeReference attr, C context) {
        return visit(attr, context);
    }

    public R visitBinaryPredicate(BinaryPredicate bp, C context) {
        return visit(bp, context);
    }

    public R visitIntLiteral(IntLiteral intLiteral, C context) {
        return visit(intLiteral, context);
    }

    public R visitInPredicate(InPredicate in, C context) {
        return visit(in, context);
    }

    public R visitSubQuery(SubQuery subQuery, C context) {
        return visit(subQuery, context);
    }
}
