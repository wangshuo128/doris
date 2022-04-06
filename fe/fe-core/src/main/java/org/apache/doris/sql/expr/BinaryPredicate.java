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

import java.util.ArrayList;
import java.util.List;

import org.apache.doris.sql.rule.ExprVisitor;

public class BinaryPredicate extends Predicate {
    public Expression left;
    public Expression right;
    public Operator op;

    public enum Operator {
        EQ("="),
        NE("!="),
        LE("<="),
        GE(">="),
        LT("<"),
        GT(">");

        private String operand;

        Operator(String operand) {
            this.operand = operand;
        }
    }

    public BinaryPredicate(Expression left, Expression right, Operator op) {
        this.left = left;
        this.right = right;
        this.op = op;
    }

    @Override
    public <R, C> R accept(ExprVisitor<R, C> visitor, C context) {
        return visitor.visitBinaryPredicate(this, context);
    }

    @Override
    public String toString() {
        return "(" + left +
                   " " + op.operand + " " +
                   right + ")";
    }

    @Override
    public List<Expression> getChildren() {
        List<Expression> children = new ArrayList<>();
        children.add(left);
        children.add(right);
        return children;
    }
}
