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

import org.apache.commons.lang.StringEscapeUtils;
import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.Instance;
import com.aliyun.odps.utils.StringUtils;

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

    String jsonSummary = "{\\n    \\\"Cost\\\": {\\n        \\\"CPU\\\": 100,\\n        \\\"GPU\\\": 0,\\n        \\\"Input\\\": 400,\\n        \\\"Memory\\\": 1024},\\n    \\\"Inputs\\\": {\\\"odpsdemo_dev.10_numbers\\\": [10,\\n            400]},\\n    \\\"Outputs\\\": {\\\"odpsdemo_dev.tmp\\\": [10,\\n            496]},\\n    \\\"Stages\\\": {\\\"M1_odpsdemo_dev_20190531085701523gkm1r3pr2_SQL_0_0_0_job_0\\\": {\\n            \\\"InputRecordCountStats\\\": {\\\"TableScan1\\\": [10,\\n                    10,\\n                    10]},\\n            \\\"InputRecordCounts\\\": {\\\"TableScan1\\\": 10},\\n            \\\"OutputRecordCountStats\\\": {\\\"TableSink1\\\": [10,\\n                    10,\\n                    10]},\\n            \\\"OutputRecordCounts\\\": {\\\"TableSink1\\\": {\\\"TableSink1\\\": 10}},\\n            \\\"UserCounters\\\": {},\\n            \\\"WorkerCount\\\": 1,\\n            \\\"WriterBytes\\\": {\\\"TableSink1\\\": {\\\"TableSink1\\\": 496}}}}}";
    Assert.assertEquals(10, Utils.getSinkCountFromTaskSummary(
        StringEscapeUtils.unescapeJava(jsonSummary)));
  }

}
