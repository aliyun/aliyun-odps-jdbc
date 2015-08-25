/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */

package com.aliyun.odps.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Assert;
import org.junit.Test;

public class Benchmark {


  @Test
  public void testWithDiffFetchSize() throws Exception {

    Connection conn = OdpsConnectionFactory.getInstance().conn;

    Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                          ResultSet.CONCUR_READ_ONLY);

    String sql = "select * from save_private_ryan limit 1000000";
    ResultSet rs = stmt.executeQuery(sql);

    // Test performance for difference fetch size
    int[] fetchSizes = {100000, 50000, 20000, 10000};
    long[] elpasedTime = new long[fetchSizes.length];

    for (int i = 0; i < fetchSizes.length; i++) {
      long start = System.currentTimeMillis();
      rs.beforeFirst();
      {
        int index = 0;
        while (rs.next()) {
          Assert.assertEquals(index + 1, rs.getRow());
          rs.getInt(1);
          index++;
        }
      }
      long end = System.currentTimeMillis();
      elpasedTime[i] = end - start;
    }

    for (int i = 0; i < fetchSizes.length; i++) {
      System.out.printf("step\t%d\tmillis\t%d\n", fetchSizes[i], elpasedTime[i]);
    }

    rs.close();
    stmt.close();
    conn.close();
  }
}
