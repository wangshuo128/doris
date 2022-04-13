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

package org.apache.doris.sql.analyzer;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.doris.sql.analysis.UnresolvedAttribute;
import org.apache.doris.sql.expr.Attribute;

/**
 * A scope is all the attributes can be seen in a query plan node.
 * `outerScope` is the outer attributes if a plan is a subquery plan.
 */
public class Scope {
    private final Optional<Scope> outerScope;
    private final List<Attribute> attrs;

    public Scope(Optional<Scope> outerScope, List<Attribute> attrs) {
        this.outerScope = outerScope;
        this.attrs = attrs;
    }

    public Scope(List<Attribute> attrs) {
        this.attrs = attrs;
        this.outerScope = Optional.empty();
    }

    public List<Attribute> resolve(UnresolvedAttribute ua) {
        List<Attribute> resolvedResult = resolve(ua, attrs);
        if (resolvedResult.isEmpty() && outerScope.isPresent()) {
            // If can't resolve filed in the current scope, try to resolve in outer scope.
            // Note that we only support loop up in parent scope, i.e., back track only one level
            // in scope hierarchy, rather than all the ancestor scopes.
            return resolve(ua, outerScope.get().attrs);
        } else {
            return resolvedResult;
        }
    }

    private List<Attribute> resolve(UnresolvedAttribute ua, List<Attribute> attrs) {
        return attrs.stream().filter(attr -> {
            if (ua.getBase().isPresent() && attr.getQualifier().isPresent()) {
                // table.col
                // System.out.println("ua name: " + ua.getBase().get() + ua.getName() +
                //                        ", attr name: " + attr.getQualifier().get() + attr.getName());
                return (ua.getBase().get() + ua.getName()).equalsIgnoreCase(
                    attr.getQualifier().get() + attr.getName());
            } else {
                // col
                return ua.getName().equalsIgnoreCase(attr.getName());
            }
        }).collect(Collectors.toList());
    }

    public static Scope of(List<Attribute> attrs) {
        return new Scope(attrs);
    }
}
