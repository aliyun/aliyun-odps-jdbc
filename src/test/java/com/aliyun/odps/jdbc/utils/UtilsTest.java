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

package com.aliyun.odps.jdbc.utils;

import java.util.Properties;

import org.apache.commons.lang.StringEscapeUtils;
import org.junit.Assert;
import org.junit.Test;

public class UtilsTest {

  @Test
  public void matchPattern() throws Exception {

    Assert.assertTrue(Utils.matchPattern("abcd", "abcd"));
    Assert.assertFalse(Utils.matchPattern("abcd", "abc"));

    Assert.assertTrue(Utils.matchPattern("abcdefg", "ab_defg"));
    Assert.assertTrue(Utils.matchPattern("abcde2g", "ab_de_g"));
    Assert.assertFalse(Utils.matchPattern("abcdefg", "abc_def"));
    Assert.assertFalse(Utils.matchPattern("", "_"));

    Assert.assertTrue(Utils.matchPattern("abcdefg", "ab%fg"));
    Assert.assertTrue(Utils.matchPattern("a_cdefg", "%"));
    Assert.assertTrue(Utils.matchPattern("abcdefg", "%%"));
    Assert.assertFalse(Utils.matchPattern("abcdefg", "z%"));

    // escape
    Assert.assertFalse(Utils.matchPattern("abc_efg", "c\\_efg"));
    Assert.assertTrue(Utils.matchPattern("abc_efg", "%c\\_efg"));
    Assert.assertFalse(Utils.matchPattern("abc_efg", "%ab\\_efg"));
    Assert.assertTrue(Utils.matchPattern("ab%efg", "%ab\\%efg"));
  }

  @Test
  public void testGetSinkCountFromTaskSummary() {

    String jsonSummary;

    jsonSummary =
        "{\\n    \\\"Cost\\\": {\\n        \\\"CPU\\\": 100,\\n        \\\"GPU\\\": 0,\\n        \\\"Input\\\": 400,\\n        \\\"Memory\\\": 1024},\\n    \\\"Inputs\\\": {\\\"odpsdemo_dev.10_numbers\\\": [10,\\n            400]},\\n    \\\"Outputs\\\": {\\\"odpsdemo_dev.tmp\\\": [10,\\n            496]},\\n    \\\"Stages\\\": {\\\"M1_odpsdemo_dev_20190531085701523gkm1r3pr2_SQL_0_0_0_job_0\\\": {\\n            \\\"InputRecordCountStats\\\": {\\\"TableScan1\\\": [10,\\n                    10,\\n                    10]},\\n            \\\"InputRecordCounts\\\": {\\\"TableScan1\\\": 10},\\n            \\\"OutputRecordCountStats\\\": {\\\"TableSink1\\\": [10,\\n                    10,\\n                    10]},\\n            \\\"OutputRecordCounts\\\": {\\\"TableSink1\\\": {\\\"TableSink1\\\": 10}},\\n            \\\"UserCounters\\\": {},\\n            \\\"WorkerCount\\\": 1,\\n            \\\"WriterBytes\\\": {\\\"TableSink1\\\": {\\\"TableSink1\\\": 496}}}}}";
    Assert.assertEquals(10, Utils.getSinkCountFromTaskSummary(
        StringEscapeUtils.unescapeJava(jsonSummary)));

    // no outputs
    jsonSummary =
        "{\\n    \\\"Cost\\\": {\\n        \\\"CPU\\\": 200,\\n        \\\"GPU\\\": 0,\\n        \\\"Input\\\": 400,\\n        \\\"Memory\\\": 3584},\\n    \\\"Inputs\\\": {\\\"odpsdemo_dev.10_numbers\\\": [10,\\n            400]},\\n    \\\"Outputs\\\": {},\\n    \\\"Stages\\\": {\\n        \\\"M1_odpsdemo_dev_2019060403073279gbnoz3pr2_SQL_0_0_0_job_0\\\": {\\n            \\\"InputRecordCountStats\\\": {\\\"TableScan1\\\": [10,\\n                    10,\\n                    10]},\\n            \\\"InputRecordCounts\\\": {\\\"TableScan1\\\": 10},\\n            \\\"OutputRecordCountStats\\\": {\\\"StreamLineWrite1\\\": [10,\\n                    10,\\n                    10]},\\n            \\\"OutputRecordCounts\\\": {\\\"StreamLineWrite1\\\": {\\\"StreamLineWrite1\\\": 10}},\\n            \\\"UserCounters\\\": {},\\n            \\\"WorkerCount\\\": 1,\\n            \\\"WriterBytes\\\": {\\\"StreamLineWrite1\\\": {\\\"StreamLineWrite1\\\": 75}}},\\n        \\\"R2_1_odpsdemo_dev_2019060403073279gbnoz3pr2_SQL_0_0_0_job_0\\\": {\\n            \\\"InputRecordCountStats\\\": {\\\"StreamLineRead1\\\": [10,\\n                    10,\\n                    10]},\\n            \\\"InputRecordCounts\\\": {\\\"StreamLineRead1\\\": 10},\\n            \\\"OutputRecordCountStats\\\": {\\\"AdhocSink1\\\": [10,\\n                    10,\\n                    10]},\\n            \\\"OutputRecordCounts\\\": {\\\"AdhocSink1\\\": {\\\"AdhocSink1\\\": 10}},\\n            \\\"UserCounters\\\": {},\\n            \\\"WorkerCount\\\": 1,\\n            \\\"WriterBytes\\\": {\\\"AdhocSink1\\\": {\\\"AdhocSink1\\\": 496}}}}}";
    Assert.assertEquals(-1, Utils.getSinkCountFromTaskSummary(
        StringEscapeUtils.unescapeJava(jsonSummary)));
  }

  @Test
  public void testParseSetting() {
    {
      String sql = "select keyvalue(f1,\";\",\":\",\"mktActivityType\") f1 from test_dirty;";
      Properties properties = new Properties();
      String realQuery = Utils.parseSetting(sql, properties);
      Assert.assertEquals(realQuery, sql);
      Assert.assertEquals(properties.size(), 0);
    }
    {
      String
          sql =
          "set 1=1;select keyvalue(f1,\";\",\":\",\"mktActivityType\") f1 from test_dirty;";
      Properties properties = new Properties();
      String realQuery = Utils.parseSetting(sql, properties);
      Assert.assertEquals(realQuery,
                          "select keyvalue(f1,\";\",\":\",\"mktActivityType\") f1 from test_dirty;");
      Assert.assertEquals(properties.size(), 1);
      Assert.assertTrue(properties.getProperty("1").equals("1"));
    }
    {
      String sql = "select 1 from test;";
      Properties properties = new Properties();
      String realQuery = Utils.parseSetting(sql, properties);
      Assert.assertEquals(realQuery, sql);
      Assert.assertEquals(properties.size(), 0);
    }
    {
      String sql = "select 1 from test";
      Properties properties = new Properties();
      String realQuery = Utils.parseSetting(sql, properties);
      Assert.assertEquals(realQuery, sql + ";");
      Assert.assertEquals(properties.size(), 0);
    }
    {
      String sql = "set 1=1;select 1 from test;";
      Properties properties = new Properties();
      String realQuery = Utils.parseSetting(sql, properties);
      Assert.assertEquals(realQuery, "select 1 from test;");
      Assert.assertEquals(properties.size(), 1);
      Assert.assertTrue(properties.getProperty("1").equals("1"));
    }
    {
      String sql = "set 1=1;set 2=2; select 1 from test;";
      Properties properties = new Properties();
      String realQuery = Utils.parseSetting(sql, properties);
      Assert.assertEquals(realQuery, "select 1 from test;");
      Assert.assertEquals(properties.size(), 2);
      Assert.assertTrue(properties.getProperty("1").equals("1"));
      Assert.assertTrue(properties.getProperty("2").equals("2"));
    }
    {
      String sql = "set 1=1";
      Properties properties = new Properties();
      String realQuery = Utils.parseSetting(sql, properties);
      Assert.assertEquals(realQuery, null);
      Assert.assertEquals(properties.size(), 1);
      Assert.assertTrue(properties.getProperty("1").equals("1"));
    }
    {
      String sql = "Set 1=1";
      Properties properties = new Properties();
      String realQuery = Utils.parseSetting(sql, properties);
      Assert.assertEquals(realQuery, null);
      Assert.assertEquals(properties.size(), 1);
      Assert.assertTrue(properties.getProperty("1").equals("1"));
    }
    {
      String sql = "set 1=1;set 2=2;";
      Properties properties = new Properties();
      String realQuery = Utils.parseSetting(sql, properties);
      Assert.assertEquals(realQuery, null);
      Assert.assertEquals(properties.size(), 2);
      Assert.assertTrue(properties.getProperty("1").equals("1"));
      Assert.assertTrue(properties.getProperty("2").equals("2"));
    }
    {
      // TODO apply this setting or ignore this setting?
      String sql = "select 1 from test;set 1=1;";
      Properties properties = new Properties();
      String realQuery = Utils.parseSetting(sql, properties);
      Assert.assertEquals(realQuery, sql);
      Assert.assertEquals(properties.size(), 0);
      Assert.assertTrue(properties.isEmpty());
    }
  }
}
