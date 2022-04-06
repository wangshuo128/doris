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

package org.apache.doris.planner;

import java.util.List;
import java.util.Optional;

import com.google.common.base.Preconditions;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.ExplainOptions;
import org.apache.doris.common.UserException;
import org.apache.doris.common.profile.PlanTreeBuilder;
import org.apache.doris.common.profile.PlanTreePrinter;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PlanPrinter {

    private static final Logger LOG = LogManager.getLogger(PlanPrinter.class);

    public static String explain(List<PlanFragment> fragments, ExplainOptions options,
                          Optional<DescriptorTable> descTable) {
        Preconditions.checkNotNull(options);
        if (options.isGraph()) {
            // print the plan graph
            PlanTreeBuilder builder = new PlanTreeBuilder(fragments);
            try {
                builder.build();
            } catch (UserException e) {
                LOG.warn("Failed to build explain plan tree", e);
                return e.getMessage();
            }
            return PlanTreePrinter.printPlanExplanation(builder.getTreeRoot());
        }

        // print text plan
        TExplainLevel explainLevel = options.isVerbose() ? TExplainLevel.VERBOSE : TExplainLevel.NORMAL;
        StringBuilder str = new StringBuilder();
        for (int i = 0; i < fragments.size(); ++i) {
            PlanFragment fragment = fragments.get(i);
            if (i > 0) {
                // a blank line between plan fragments
                str.append("\n");
            }
            str.append("PLAN FRAGMENT " + i + "\n");
            str.append(fragment.getExplainString(explainLevel));
        }
        if (explainLevel == TExplainLevel.VERBOSE) {
            DescriptorTable desc =
                descTable.orElseThrow(() -> new RuntimeException("DescTable must exist when explain level is VERBOSE"));
            str.append(desc.getExplainString());
        }
        return str.toString();
    }
}
