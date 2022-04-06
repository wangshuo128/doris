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

package org.apache.doris.sql.tree;

import java.util.Collections;
import java.util.List;

import com.google.common.base.Predicate;

public abstract class TreeNode<NodeType extends TreeNode<NodeType>> {

    public List<NodeType> getChildren() {
        return Collections.emptyList();
    }

    /**
     * Add all nodes in the tree that satisfy 'predicate' to the list 'matches'
     * This node is checked first, followed by its children in order. All nodes
     * that match in the subtree are added.
     */
    public <C extends TreeNode<NodeType>, D extends C> void collectAll(
        Predicate<? super C> predicate, List<D> matches) {
        if (predicate.apply((C) this)) {
            matches.add((D) this);
        }
        for (NodeType child : getChildren()) {
            child.collectAll(predicate, matches);
        }
    }

}
