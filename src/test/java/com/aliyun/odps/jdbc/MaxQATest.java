package com.aliyun.odps.jdbc;

import java.sql.Connection;

import org.junit.Test;

import com.aliyun.odps.jdbc.utils.TestUtils;
import com.google.common.collect.ImmutableMap;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class MaxQATest {

  @Test
  public void testMaxQA() throws Exception {
    Connection
        connection =
        TestUtils.getConnection(
            ImmutableMap.of("interactiveMode", "MaxQA", "quotaName", "maxqa_tpcds_test_32cu"));
    boolean execute = connection.createStatement().execute("select 1;");
  }

}
