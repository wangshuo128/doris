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

package org.apache.doris.sql.parser;

import com.google.common.base.MoreObjects;
import org.apache.doris.sql.plan.logical.LogicalPlan;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;

public class SqlParser {

    public LogicalPlan parse(String sql) throws Exception {
        DorisSqlLexer lexer = new DorisSqlLexer(new CaseInsensitiveStream(CharStreams.fromString(sql)));

        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        DorisSqlParser parser = new DorisSqlParser(tokenStream);

//        parser.addParseListener(PostProcessor)
//        parser.removeErrorListeners()
//        parser.addErrorListener(ParseErrorListener)

        ParserRuleContext tree;
        try {
            // first, try parsing with potentially faster SLL mode
            parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
            tree = parser.statement();
        } catch (ParseCancellationException ex) {
            // if we fail, parse with LL mode
            tokenStream.seek(0); // rewind input stream
            parser.reset();

            parser.getInterpreter().setPredictionMode(PredictionMode.LL);
            tree = parser.statement();
        }

        AstBuilder astBuilder = new AstBuilder();
        return (LogicalPlan) astBuilder.visit(tree);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                   .toString();
    }
}
