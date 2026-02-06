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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Properties;
import java.util.TimeZone;

import org.apache.commons.lang3.StringEscapeUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class UtilsTest {

  @Test
  public void matchPattern() throws Exception {

    Assertions.assertTrue(Utils.matchPattern("abcd", "abcd"));
    Assertions.assertFalse(Utils.matchPattern("abcd", "abc"));

    Assertions.assertTrue(Utils.matchPattern("abcdefg", "ab_defg"));
    Assertions.assertTrue(Utils.matchPattern("abcde2g", "ab_de_g"));
    Assertions.assertFalse(Utils.matchPattern("abcdefg", "abc_def"));
    Assertions.assertFalse(Utils.matchPattern("", "_"));

    Assertions.assertTrue(Utils.matchPattern("abcdefg", "ab%fg"));
    Assertions.assertTrue(Utils.matchPattern("a_cdefg", "%"));
    Assertions.assertTrue(Utils.matchPattern("abcdefg", "%%"));
    Assertions.assertFalse(Utils.matchPattern("abcdefg", "z%"));

    // escape
    Assertions.assertFalse(Utils.matchPattern("abc_efg", "c\\_efg"));
    Assertions.assertTrue(Utils.matchPattern("abc_efg", "%c\\_efg"));
    Assertions.assertFalse(Utils.matchPattern("abc_efg", "%ab\\_efg"));
    Assertions.assertTrue(Utils.matchPattern("ab%efg", "%ab\\%efg"));
  }

  @Test
  public void testGetSinkCountFromTaskSummary() {

    String jsonSummary;

    jsonSummary =
        "{\\n    \\\"Cost\\\": {\\n        \\\"CPU\\\": 100,\\n        \\\"GPU\\\": 0,\\n        \\\"Input\\\": 400,\\n        \\\"Memory\\\": 1024},\\n    \\\"Inputs\\\": {\\\"odpsdemo_dev.10_numbers\\\": [10,\\n            400]},\\n    \\\"Outputs\\\": {\\\"odpsdemo_dev.tmp\\\": [10,\\n            496]},\\n    \\\"Stages\\\": {\\\"M1_odpsdemo_dev_20190531085701523gkm1r3pr2_SQL_0_0_0_job_0\\\": {\\n            \\\"InputRecordCountStats\\\": {\\\"TableScan1\\\": [10,\\n                    10,\\n                    10]},\\n            \\\"InputRecordCounts\\\": {\\\"TableScan1\\\": 10},\\n            \\\"OutputRecordCountStats\\\": {\\\"TableSink1\\\": [10,\\n                    10,\\n                    10]},\\n            \\\"OutputRecordCounts\\\": {\\\"TableSink1\\\": {\\\"TableSink1\\\": 10}},\\n            \\\"UserCounters\\\": {},\\n            \\\"WorkerCount\\\": 1,\\n            \\\"WriterBytes\\\": {\\\"TableSink1\\\": {\\\"TableSink1\\\": 496}}}}}";
    assertEquals(10, Utils.getSinkCountFromTaskSummary(
        StringEscapeUtils.unescapeJava(jsonSummary)));

    // no outputs
    jsonSummary =
        "{\\n    \\\"Cost\\\": {\\n        \\\"CPU\\\": 200,\\n        \\\"GPU\\\": 0,\\n        \\\"Input\\\": 400,\\n        \\\"Memory\\\": 3584},\\n    \\\"Inputs\\\": {\\\"odpsdemo_dev.10_numbers\\\": [10,\\n            400]},\\n    \\\"Outputs\\\": {},\\n    \\\"Stages\\\": {\\n        \\\"M1_odpsdemo_dev_2019060403073279gbnoz3pr2_SQL_0_0_0_job_0\\\": {\\n            \\\"InputRecordCountStats\\\": {\\\"TableScan1\\\": [10,\\n                    10,\\n                    10]},\\n            \\\"InputRecordCounts\\\": {\\\"TableScan1\\\": 10},\\n            \\\"OutputRecordCountStats\\\": {\\\"StreamLineWrite1\\\": [10,\\n                    10,\\n                    10]},\\n            \\\"OutputRecordCounts\\\": {\\\"StreamLineWrite1\\\": {\\\"StreamLineWrite1\\\": 10}},\\n            \\\"UserCounters\\\": {},\\n            \\\"WorkerCount\\\": 1,\\n            \\\"WriterBytes\\\": {\\\"StreamLineWrite1\\\": {\\\"StreamLineWrite1\\\": 75}}},\\n        \\\"R2_1_odpsdemo_dev_2019060403073279gbnoz3pr2_SQL_0_0_0_job_0\\\": {\\n            \\\"InputRecordCountStats\\\": {\\\"StreamLineRead1\\\": [10,\\n                    10,\\n                    10]},\\n            \\\"InputRecordCounts\\\": {\\\"StreamLineRead1\\\": 10},\\n            \\\"OutputRecordCountStats\\\": {\\\"AdhocSink1\\\": [10,\\n                    10,\\n                    10]},\\n            \\\"OutputRecordCounts\\\": {\\\"AdhocSink1\\\": {\\\"AdhocSink1\\\": 10}},\\n            \\\"UserCounters\\\": {},\\n            \\\"WorkerCount\\\": 1,\\n            \\\"WriterBytes\\\": {\\\"AdhocSink1\\\": {\\\"AdhocSink1\\\": 496}}}}}";
    assertEquals(-1, Utils.getSinkCountFromTaskSummary(
        StringEscapeUtils.unescapeJava(jsonSummary)));
  }

  @Test
  public void testParseSetting() {
    {
      String sql = "cost sql select 1; \n";
      Properties properties = new Properties();
      String realSql = Utils.parseSetting(sql, properties);
      assertEquals(realSql, sql.trim());
      assertEquals(properties.size(), 0);
    }
    {
      String sql = "select keyvalue(f1,\";\",\":\",\"mktActivityType\") f1 from test_dirty;";
      Properties properties = new Properties();
      String realQuery = Utils.parseSetting(sql, properties);
      assertEquals(realQuery, sql);
      assertEquals(properties.size(), 0);
    }
    {
      String
          sql =
          "set 1=1;select keyvalue(f1,\";\",\":\",\"mktActivityType\") f1 from test_dirty;";
      Properties properties = new Properties();
      String realQuery = Utils.parseSetting(sql, properties);
      assertEquals(realQuery,
                          "select keyvalue(f1,\";\",\":\",\"mktActivityType\") f1 from test_dirty;");
      assertEquals(properties.size(), 1);
      Assertions.assertTrue(properties.getProperty("1").equals("1"));
    }
    {
      String sql = "select 1 from test;";
      Properties properties = new Properties();
      String realQuery = Utils.parseSetting(sql, properties);
      assertEquals(realQuery, sql);
      assertEquals(properties.size(), 0);
    }
    {
      String sql = "select 1 from test";
      Properties properties = new Properties();
      String realQuery = Utils.parseSetting(sql, properties);
      assertEquals(realQuery, sql + ";");
      assertEquals(properties.size(), 0);
    }
    {
      String sql = "set 1=1;select 1 from test;";
      Properties properties = new Properties();
      String realQuery = Utils.parseSetting(sql, properties);
      assertEquals(realQuery, "select 1 from test;");
      assertEquals(properties.size(), 1);
      Assertions.assertTrue(properties.getProperty("1").equals("1"));
    }
    {
      String sql = "set 1=1;set 2=2; select 1 from test;";
      Properties properties = new Properties();
      String realQuery = Utils.parseSetting(sql, properties);
      assertEquals(realQuery, "select 1 from test;");
      assertEquals(properties.size(), 2);
      Assertions.assertTrue(properties.getProperty("1").equals("1"));
      Assertions.assertTrue(properties.getProperty("2").equals("2"));
    }
    {
      String sql = "set 1=1";
      Properties properties = new Properties();
      String realQuery = Utils.parseSetting(sql, properties);
      assertEquals(realQuery, null);
      assertEquals(properties.size(), 1);
      Assertions.assertTrue(properties.getProperty("1").equals("1"));
    }
    {
      String sql = "Set 1=1";
      Properties properties = new Properties();
      String realQuery = Utils.parseSetting(sql, properties);
      assertEquals(realQuery, null);
      assertEquals(properties.size(), 1);
      Assertions.assertTrue(properties.getProperty("1").equals("1"));
    }
    {
      String sql = "set 1=1;set 2=2;";
      Properties properties = new Properties();
      String realQuery = Utils.parseSetting(sql, properties);
      assertEquals(realQuery, null);
      assertEquals(properties.size(), 2);
      Assertions.assertTrue(properties.getProperty("1").equals("1"));
      Assertions.assertTrue(properties.getProperty("2").equals("2"));
    }
    {
      // TODO apply this setting or ignore this setting?
      String sql = "select 1 from test;set 1=1;";
      Properties properties = new Properties();
      String realQuery = Utils.parseSetting(sql, properties);
      assertEquals(realQuery, sql);
      assertEquals(properties.size(), 0);
      Assertions.assertTrue(properties.isEmpty());
    }
  }

  @Test
  public void testParseSettingWithComments() {
    String comment1 = "-- I am Comment1; not over, over now.\n";
    String comment2 = "-- I am Comment2\n";
    String comment3 = "/* 我是多行注释; \n" + "多行注释结束 */ ";
    String setting1 = "set 1= 1;\n --comments\n";
    String setting2 = "set 2='2'; -- set 3=3; \n";
    String comment4 = "/*+ 我不是多行注释, 我是hints */ ";
    String query = "select 1;";

    String sql = comment1 + comment2 + setting1 + comment3 + setting2 + comment2 + comment4 + query;

    Properties properties = new Properties();
    String res = Utils.parseSetting(sql, properties);

    assertEquals(comment4 + query, res);
    assertEquals(properties.size(), 2);
    assertEquals(properties.getProperty("1"), "1");
    assertEquals(properties.getProperty("2"), "'2'");
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

    assertEquals(sqlExpect, res);
    assertEquals(properties.size(), 2);
    assertEquals(properties.getProperty("odps.namespace.schema"), "true");
    assertEquals(properties.getProperty("odps.sql.validate.orderby.limit"), "false");

  }

  @Test
  public void testLargeSql() {
    Properties properties = new Properties();
    String sql = "select *\n" +
            "from values ('8*10-7*8,8-8*(7-9),8*9/(10-7),8+(10-8)*8,(1+1+1)*8,2*(1+10+1),6*(2+1+1),(1+2)*(1+7),(2+1)*8*1,(9-1)*(2+1),3*(10-1-1),(1+1)*4*3,(3+1)*(5+1),(3+1)*6*1,(1+7)*1*3,8*3*1*1,3*1*(9-1),4+(1+1)*10,4*(4+1+1),1*(1+5)*4,6*1*4*1,(7-1)*1*4,1*8*(4-1),(1-9)*(1-4),(5-1)*(5+1),6*1*(5-1),(1+1)*(7+5),(5-1-1)*8,6*(6-1-1),8*6/(1+1),6+9*(1+1),10+7*(1+1),8+(1+1)*8,2*(2+10*1),4*2*(1+2),2*2*(5+1),(2+2)*1*6,2*2*(7-1),8*(2-1+2),(9+1+2)*2,3+2*10+1,2*(1+3)*3,4*3*2*1,(2+5+1)*3,6*2*(3-1),3*(7+2-1),(2-1)*8*3,(3+9)*2*1,2*10+4*1,(2+4)*4*1,(2-1+5)*4,(6+2)*(4-1),2*(1+4+7),2*(8+4*1),2*(4-1+9),2*10+5-1,5*5+1-2,6*(5-2+1),2*1*(7+5),(8+5-1)*2,5+1+2*9,(2+1)*10-6,2*(6+1*6),(6-2)*(7-1),8*6/2*1,1*2*9+6,10+2*1*7,(7*7-1)/2,8*2+1+7,9+7*2+1,10-(1-8)*2,(8/2-1)*8,8*9/(1+2),10+3+10+1,3*(10-3+1),(3+1)*(3+3),(3*1+3)*4,(1*5+3)*3,(6-1+3)*3,1*7*3+3,3+(8-1)*3,(3*9-3)*1,4-(1-3)*10,4*(3-1+4),5*4+1+3,6/(1-3/4),(1+3)*7-4,8/(4/3-1),1-4+9*3,3*5+10-1,1+5+6*3,(3-1)*(5+7),3*5+1+8,5*3+1*9,10*3*1-6,1*6*3+6,1*(7-3)*6,6*8/(3-1),6+(3-1)*9,1-7+3*10,(3-7)*(1-7),8*(7-1-3),(1+7)*9/3,8/3*(10-1),8/3*(8+1),8*9*1/3,(10+1)*3-9,9*(9-1)/3,10+10+4*1,4*(10-4)*1,4+4*(1+4),1*5*4+4,(6-1)*4+4,1*7*4-4,4*1*4+8,9-1+4*4,(10-4)*(5-1),5*4+5-1,4/(1-5/6),4*7-5+1,(8-4)*(5+1),9+(4-1)*5,(4-1)*10-6,6-(1-4)*6,6*(7-4+1),(8-4)*6*1,6*(9-1-4),(7-4)*(1+7),(7-4)*1*8,(4-7)*(1-9),4*8*1-8,4*8+1-9,10+9+4+1,10+5-1+10,(10-5)*5-1,5*(5-1/5),6*5-1-5,(9-5)*(5+1),(1+5)*(10-6),1*(6*5-6),6*5+1-7,6*(1-5+8),(9-5)*1*6,7*5-1-10,(1-5+7)*8,(7-1)*(9-5),8+10+1+5,(5-1)*8-8,(5-8)*(1-9),10+5+9*1,9+9+5+1,(1*10-6)*6,(6-1)*6-6,6/(1-6/8),6*(1+9-6),6*(10+1-7),(7+1)*(9-6),8+1*10+6,8*(8+1-6),1+8+6+9,6+10+9-1,1*9+9+6,7*1+10+7,1+7+9+7,10+7-1+8,1+8+8+7,8*(1+9-7),(9-1)*(10-7),9-1+9+7,(1-8+10)*8,8*1+8+8,8-1+9+8,2+2+10+10,10*2+2+2,2*2*2*3,4*(2+2+2),2*(2+2*5),(2*7-2)*2,2*(2+8+2),2+(9+2)*2,(3+10)*2-2,2*(3+3)*2,3*(2+4+2),3*(5*2-2),(6-2)*3*2,(2+7+3)*2,8*3*2/2,2*3+2*9,(4-2)*(10+2),(2*4+4)*2,(2/2+5)*4,4*6-2+2,7*4-2-2,(8/2+2)*4,2+4+2*9,(5+2)*2+10,(5+2+5)*2,2+2*(5+6),2*7+5*2,(5+8)*2-2,2*(5+9-2),2+10+6*2,6*(2+6)/2,6+(7+2)*2,(8+6-2)*2,(9*2-6)*2,2*(10/2+7),2*(7+7-2),2+8+2*7,10*2+8/2,8*(8-2)/2,9*2+8-2,10-2*(2-9),(10-3)*2+10,(10/2+3)*3,(2+3+3)*3,2*(5*3-3),2*(3+6+3),3*(3-2+7),(3+3)*8/2,(9-3+2)*3,4*3+2+10,3*4/2*4,2*(3+4+5),3*(6+4/2),(7-3)*(2+4),(8-4)*2*3,2/3*9*4,2*(10-3+5),3*(5-2+5),2*5*3-6,5+7*3-2,5+3+2*8,(2-5)+9*3,(10-2)*(6-3),2*6/3*6,(6/2)+7*3,8+6*3-2,3+9+6*2,7+10*2-3,7+2*7+3,2*(7+8-3),9-3*(2-7),10+8+2*3,(8-3-2)*8,8*(9-2*3),2+3+9+10,2*3+9+9,10*(4/10+2),(4-2)*10+4,4*(4+4-2),(5*2-4)*4,2+6+4*4,2*4*(7-4),(8+4)*4/2,(9-2)*4-4,10+4+2*5,4+(5+5)*2,4*5+6-2,(7+5)/2*4,2*4*(8-5),(2+4)*(9-5),(2-6+10)*4,6*(6-4+2),4+6+2*7,6*2/4*8,(9-6)*2*4,10+7*4/2,(7+7)*2-4,8/2*7-4,7+9+2*4,8+2+10+4,8-8*(2-4),8*(9-4-2),9*2-4+10,2+4+9+9,2*(10/5+10),5*(5-2/10),7*2+5+5,(5/5+2)*8,5+5*2+9,6*2*10/5,6*(5*2-6),6*(7-5+2),2+5*6-8,6*5/2+9,2+10+7+5,7+5*2+7,(5*2-7)*8,7*5-2-9,(10-5-2)*8,8*5-8*2,2*(9-5+8),9+10*2-5,10+6+10-2,2+6+10+6,6+6*6/2,6*(7-6/2),6*(2+8-6),(6+2)*(9-6),(10-7)*(6+2),8*(2-6+7),7+6+9+2,(6-10)*(2-8),6+8+8+2,9*8*2/6,(10-2)*(9-6),2*(9+9-6),(10-7)*(10-2),7*(10/7+2),7+2+8+7,(8-7+2)*8,(9+7)*2-8,10-2+7+9,(10-8)*(10+2),8+10-2+8,8*(8/8+2),8*(9+2-8),8*(10+2-9),9+9+8-2,10/2+10+9,3+(10-3)*3,3*3*3-3,(3+4)*3+3,3*3+3*5,6*3+3+3,(7-3)*(3+3),3*8*3/3,3*(9-3/3),(3*4-4)*3,(5+4)*3-3,4*(3-3+6),3*(4-3+7),3*8*(4-3),3+9+3*4,10+3*3+5,5*5-3/3,(6-3)*(3+5),3*(5*3-7),(5+3)/3*9,3*(10-6/3),3*(6+6/3),3-(3-6)*7,8*(6+3)/3,9-(3-3*6),7*(3+3/7),7+8+3*3,(3-7)*(3-9),3+3+8+10,8/(3-8/3),9-3*(3-8),3+10*3-9,9+3+9+3,10*3-10+4,(10-3)*4-4,(3+4)*4-4,3+5+4*4,(6-4)*4*3,4*(3-4+7),3*(4+8-4),9*4-4*3,3*4/5*10,3*5+5+4,(5-4+3)*6,(5+7-4)*3,(3+5)*4-8,(9-5+4)*3,10-4+6*3,3*4+6+6,(4+8)/3*6,4*(9-6+3),(7-3)*(10-4),3-7+4*7,8-(3-7)*4,(7+4)*3-9,(8+10)*4/3,4+9+8+3,(9+9)*4/3,3*(10-10/5),6*(3+5/5),(7+5)*(5-3),8*3*5/5,3*(9-5/5),(10/5+6)*3,6*(6-5+3),(5+7)/3*6,3*8*(6-5),3*(9-6+5),10-(3-5)*7,5*7-8-3,3+5+9+7,5+8+8+3,5-8+3*9,9+3*(10-5),9+9*5/3,10*(3-6/10),(6-3)*10-6,6*(3+6/6),(6/6+7)*3,8*6*3/6,3+6+6+9,10+6*7/3,(7/7+3)*6,(8-7+3)*6,3*(6+9-7),(6+10-8)*3,8+8/3*6,8*9/(6-3),(3-9+10)*6,(9-6)*9-3,10-3+10+7,3*7-7+10,7+7+7+3,8*(3-7+7),3*(9-7/7),3*8*(8-7),(7+9-8)*3,7-(10-9*3),3*(7+9/9),10*3/10*8,(10*8-8)/3,8*8*3/8,3*(9-8/8),9+8+10-3,3*8+9-9,3*(9-10/10),3*(9-10+9),9+9+9-3,(10*10-4)/4,10*4-4*4,4+4*4+4,4*(5+4/4),4*4/4*6,(7-4)*(4+4),4*8-4-4,4-4*(4-9),4+4*(10-5),4*(5+5-4),4*6*(5-4),(7-5+4)*4,(4+4)*(8-5),4+4+6+10,(6-4)*(8+4),4*9*4/6,(10-7)*(4+4),7*(4-4/7),4*7-8+4,7+4+4+9,4*(10+4-8),8+8+4+4,9*4-4-8,10/5*10+4,5+5+4+10,5*5-5+4,6*4*5/5,4*(7-5/5),8*(4-5/5),5*4-5+9,(10-6)+4*5,6*(6-5)*4,(6-4)*(5+7),8*(4+5-6),4+6+9+5,4+10*(7-5),4*(7/7+5),5+8+4+7,9-(4-7)*5,(4+8)*10/5,4*(8/8+5),(9+5-8)*4,4*(5+10-9),4*(9/9+5),6*4*10/10,(10+6)/4*6,(6-6+6)*4,6*4*(7-6),6+4+8+6,(9-4)*6-6,4*7+6-10,6+4+7+7,(6+4-7)*8,(7+9)/4*6,4+10*(8-6),8*6/8*4,6+8/4*9,(10*9+6)/4,4*9/9*6,4*(7-10/10),4*(7-7/7),8*(4-7/7),10+8*7/4,8*7-4*8,9*8/(7-4),4-10*(7-9),4*(7-9/9),8+10-4+10,(8+4)*(10-8),8*(4-8/8),(8+4-9)*8,8*(9+4-10),8*(4-9/9),9-(4-9-10),5*5-10/10,5*5-5/5,5-6+5*5,5+9+5+5,5*(6-6/5),7*5-5-6,5+5+8+6,(5+7)/5*10,(5+7)*(7-5),8*(5+5-7),(10+5)*8/5,5*5-8/8,8-9+5*5,9+5*5-10,5*5-9/9,(10+10)*6/5,(6+6)*10/5,6*(5-6/6),7+6+5+6,6+6*(8-5),9*6-6*5,6*(5-7/7),6*(5-8+7),6+(7-5)*9,8*5-6-10,8*(6+5-8),6*(8-9+5),6*(9-10+5),(9-6)*5+9,10+10*7/5,(7-5)*7+10,(7+5)*(9-7),(10-8)*(5+7),(7+8)*8/5,5*8-7-9,9+5*(10-7),(5-10+8)*8,5*8-8-8,9*8/(8-5),9+10-5+10,10+10+10-6,10*6-6*6,6+6+6+6,(6+6)*(8-6),6+(9-6)*6,6-(7-10)*6,6*(7+6-9),6*(8-10+6),6*8/(8-6),6-(6-8)*9,(10+6)*9/6,(10-7)*10-6,6*(7+7-10),10-7*(6-8),6*8/(9-7),9/6*(9+7),(10-6)*8-8,8+(8-6)*8,9*8-8*6');";
    assertEquals(sql, Utils.parseSetting(sql, properties));
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

    assertEquals(query, res);
    assertEquals(properties.size(), 2);
    assertEquals(properties.getProperty("1"), "1");
    assertEquals(properties.getProperty("2"), "'2'");

    properties = new Properties();
    res = Utils.parseSetting(sqlWithoutSet, properties);
    assertEquals(query, res);
    assertEquals(properties.size(), 0);

  }

  @Test
  public void testColumnNameWithSharp() {
    String sql = "create table sharp_test( `hap#user_id` bigint);";
    String res = Utils.parseSetting(sql, new Properties());
    System.out.println(res);
    assertEquals("create table sharp_test( `hap#user_id` bigint);", res);
  }

  @Test
  public void testRetrieveVersion() {
    // Test that retrieveVersion doesn't throw exceptions and returns a string
    assertDoesNotThrow(() -> {
      String version = Utils.retrieveVersion("driver.version");
      assertNotNull(version);
    });

    // Test with unknown key
    assertDoesNotThrow(() -> {
      String version = Utils.retrieveVersion("unknown.key");
      assertEquals("unknown", version);
    });
  }

  @Test
  public void testConvertToSqlType() {
    // Test null conversion
    assertNull(Utils.convertToSqlType(null, String.class, TimeZone.getDefault()));

    // Test string conversion
    assertEquals("test", Utils.convertToSqlType("test", String.class, TimeZone.getDefault()));

    // Test that method exists and can be called
    assertDoesNotThrow(() -> {
      Utils.convertToSqlType(new java.util.Date(), java.time.LocalDateTime.class, TimeZone.getDefault());
    });
  }
}
