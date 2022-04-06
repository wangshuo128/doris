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
import java.util.stream.Collectors;

import org.apache.doris.sql.analysis.UnresolvedAttribute;
import org.apache.doris.sql.expr.Attribute;

public class ResolveUtils {

    // todo: add index from id to attr in plan output
    public static List<Attribute> resolveAttr(UnresolvedAttribute ua, List<Attribute> attrs) {
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
}
