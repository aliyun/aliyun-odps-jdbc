package com.aliyun.odps.jdbc;

import java.sql.ResultSet;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class TimezoneTest {

  public static void main(String[] args) throws Exception {
    TestManager instance = TestManager.getInstance();
    ResultSet resultSet = instance.conn.createStatement().executeQuery("select now();");
    if (resultSet.next()) {
      Object o = resultSet.getTimestamp(1);
      System.out.println(o);
    }
  }
}
