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

package org.apache.doris.parser;

import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.qe.SqlModeHelper;

import org.junit.Test;

import java.io.StringReader;

public class SqlParserTest {

    @Test
    public void testParse() throws Exception {
        // SSB Q2.2
        String sql = "select sum(lo_revenue) as lo_revenue, d_year, p_brand\n" +
            "from lineorder\n" +
            "join dates on lo_orderdate = d_datekey\n" +
            "join part on lo_partkey = p_partkey\n" +
            "join supplier on lo_suppkey = s_suppkey\n" +
            "where p_brand between 'MFGR#2221' and 'MFGR#2228' and s_region = 'ASIA'\n" +
            "group by d_year, p_brand\n" +
            "order by d_year, p_brand;";
        SqlScanner input = new SqlScanner(new StringReader(sql), SqlModeHelper.MODE_DEFAULT);
        SqlParser parser = new SqlParser(input);
        SelectStmt stmt = (SelectStmt) SqlParserUtils.getFirstStmt(parser);
        System.out.println(stmt.toSql());
        // SELECT  FROM `lineorder`  INNER JOIN `dates` ON `lo_orderdate` = `d_datekey`  INNER JOIN `part` ON `lo_partkey` = `p_partkey`  INNER JOIN `supplier` ON `lo_suppkey` = `s_suppkey` WHERE `p_brand` BETWEEN 'MFGR#2221' AND 'MFGR#2228' AND `s_region` = 'ASIA' GROUP BY `d_year`, `p_brand` ORDER BY `d_year`, `p_brand`
    }
}
