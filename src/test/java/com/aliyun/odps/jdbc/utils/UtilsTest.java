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

  @Test
  public void testParseSettingWithComments() {
    String comment1 = "-- I am Comment1; not over, over now.\n";
    String comment2 = "-- I am Comment2\n";
    String comment3 = "/* 我是多行注释; \n" + "多行注释结束 */ ";
    String setting1 = "set 1= 1;\n --comments\n";
    String setting2 = "set 2='2'; -- set 3=3; \n";
    String query = "select 1;";

    String sql = comment1 + comment2 + setting1 + comment3 + setting2 + comment2 + query;

    Properties properties = new Properties();
    String res = Utils.parseSetting(sql, properties);

    Assert.assertEquals(query, res);
    Assert.assertEquals(properties.size(), 2);
    Assert.assertEquals(properties.getProperty("1"), "1");
    Assert.assertEquals(properties.getProperty("2"), "'2'");
  }

  @Test
  public void testParseSqlWithComments() {
    String sql = "set odps.namespace.schema=true; --test \n"
                 + "SET odps.sql.validate.orderby.limit = false;\n"
                 + "-- MaxComputeTutorial: Better Github Analytics Query \n"
                 + "-- ********************************************************************--\n"
                 + "-- 大量开发人员在GitHub上进行开源项目的开发工作，并在项目的开发过程中产生海量事件。GitHub会记录每次事件的类型及详情、开发者、代码仓库等信息，并开放其中的公开事件，包括加星标、提交代码等，具体事件类型请参见https://docs.github.com/en/webhooks-and-events/webhooks/webhook-events-and-payloads。\n"
                 + "\n"
                 + "-- MaxCompute将GH Archive提供的海量公开事件数据进行离线处理并开发，生成一张事实表dwd_github_events_odps;一张聚合表dws_overview_by_repo_by_month_dailyupdate\n"
                 + "-- dwd_github_events_odps:存储了每一条事件的主干信息，T+1小时更新\n"
                 + "-- dws_overview_by_repo_by_month_dailyupdate：存储了项目维度每月事件指标汇总T+1天更新\n"
                 + "-- 感谢数据源提供方GH Archive：https://www.gharchive.org/与Github API：https://docs.github.com/en/rest/activity/events。\n"
                 + "\n"
                 + "-- 公开数据集的数据均存储在一个名为MAXCOMPUTE_PUBLIC_DATA的项目中，但所有用户并未被加入到该项目中，即非项目空间成员。\n"
                 + "-- 因此，用户需要跨项目访问数据，在编写SQL脚本时，必须在表名前指定项目名称及Schema名称，本query查询的数据存储的Schema名称为：github_evnets。\n"
                 + "\n"
                 + "--  如果您未开启租户级别Schema语法，需要在运行query前通过session flag的方式设置 SET odps.namespace.schema = true;\n"
                 + "\n"
                 + "\n"
                 + "-- Better Github Analytics Query 1：统计过去一年获星标数项目排行榜（前10）\n"
                 + "SELECT\n"
                 + "    repo_id,\n"
                 + "    repo_name,\n"
                 + "    COUNT(actor_login) total\n"
                 + "FROM\n"
                 + "    maxcompute_public_data.github_events.dwd_github_events_odps\n --test\n"
                 + "WHERE\n"
                 + "    ds>=date_add(getdate(), -365)\n"
                 + "    AND type = 'WatchEvent'\n"
                 + "    AND c = 'a'\n"
                 + "    AND d = \"e\" \n"
                 + "GROUP BY\n"
                 + "    repo_id,\n"
                 + "    repo_name\n"
                 + "ORDER BY\n"
                 + "    total DESC\n"
                 + "LIMIT 10;";

    String sqlExpect = "SELECT\n"
                       + "    repo_id,\n"
                       + "    repo_name,\n"
                       + "    COUNT(actor_login) total\n"
                       + "FROM\n"
                       + "    maxcompute_public_data.github_events.dwd_github_events_odps\n"
                       + " \n"
                       + "WHERE\n"
                       + "    ds>=date_add(getdate(), -365)\n"
                       + "    AND type = 'WatchEvent'\n"
                       + "    AND c = 'a'\n"
                       + "    AND d = \"e\" \n"
                       + "GROUP BY\n"
                       + "    repo_id,\n"
                       + "    repo_name\n"
                       + "ORDER BY\n"
                       + "    total DESC\n"
                       + "LIMIT 10;";

    Properties properties = new Properties();
    String res = Utils.parseSetting(sql, properties);

    Assert.assertEquals(sqlExpect, res);
    Assert.assertEquals(properties.size(), 2);
    Assert.assertEquals(properties.getProperty("odps.namespace.schema"), "true");
    Assert.assertEquals(properties.getProperty("odps.sql.validate.orderby.limit"), "false");

  }

  @Test
  public void testParseSettingWithoutComments() {
    String setting1 = "set 1= 1;\n";
    String setting2 = "set 2='2';\n";
    String query = "select 1;";

    String sqlWithSet = setting1 + setting2 + query;
    String sqlWithoutSet = query;

    Properties properties = new Properties();
    String res = Utils.parseSetting(sqlWithSet, properties);

    Assert.assertEquals(query, res);
    Assert.assertEquals(properties.size(), 2);
    Assert.assertEquals(properties.getProperty("1"), "1");
    Assert.assertEquals(properties.getProperty("2"), "'2'");

    properties = new Properties();
    res = Utils.parseSetting(sqlWithoutSet, properties);
    Assert.assertEquals(query, res);
    Assert.assertEquals(properties.size(), 0);

  }
}
