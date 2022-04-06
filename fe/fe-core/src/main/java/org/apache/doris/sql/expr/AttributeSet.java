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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.base.Objects;
import org.apache.commons.lang3.StringUtils;

public class AttributeSet {

    private Set<Attribute> attrs = new HashSet<>();

    public AttributeSet(Set<Attribute> attrs) {
        this.attrs = attrs;
    }

    public AttributeSet(Attribute... attrs) {
        Set<Attribute> set = new HashSet<>();
        for (Attribute attr : attrs) {
            set.add(attr);
        }
        this.attrs = set;
    }

    public AttributeSet(List<Attribute> attrs) {
        this.attrs.addAll(attrs);
    }

    public boolean contains(Attribute attr) {
        return attrs.contains(attr);
    }

    public boolean contains(AttributeSet other) {
        return other.getAttrs().stream().allMatch(this::contains);
    }

    public Set<Attribute> getAttrs() {
        return attrs;
    }

    public static AttributeSet fromAttributeSets(List<AttributeSet> sets) {
        Set<Attribute> merged = new HashSet<>();
        for (AttributeSet set : sets) {
            merged.addAll(set.getAttrs());
        }
        return new AttributeSet(merged);
    }

    public static AttributeSet fromAttributeSets(AttributeSet... sets) {
        return fromAttributeSets(Arrays.asList(sets));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AttributeSet)) return false;
        AttributeSet that = (AttributeSet) o;
        return Objects.equal(getAttrs(), that.getAttrs());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getAttrs());
    }

    @Override
    public String toString() {
        return "AttributeSets(" + StringUtils.join(attrs, ", ") + ")";
    }
}
