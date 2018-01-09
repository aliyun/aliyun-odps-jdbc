package com.aliyun.odps.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JdbcTest {

  public static void main(String[] args) throws SQLException {
    if (args.length < 2) {
      System.out.println("Usage: java -cp odps-jdbc-...-jar-with-dependencies.jar com.aliyun.odps.jdbc.JdbcTest connection_string sql");
      System.out.println(
          "   eg. JdbcTest 'jdbc:odps:http://service.odps.aliyun.com/api?project=odpsdemo?accessId=...&accessKey=...&charset=UTF-8' 'select * from dual'");
      System.exit(1);
    }
    String connectionString = args[0];
    String sql = args[1];

    try {
      String driverName = "com.aliyun.odps.jdbc.OdpsDriver";
      Class.forName(driverName);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      System.exit(1);
    }

    System.out.println("Connection: " + connectionString);
    Connection conn = DriverManager.getConnection(connectionString);

    Statement stmt = conn.createStatement();
    System.out.println("Running   : " + sql);
    ResultSet res = stmt.executeQuery(sql);
    System.out.println("Result    :");
    int columnCount = res.getMetaData().getColumnCount();
    while (res.next()) {
      for (int i = 0; i < columnCount; i++) {
        System.out.print(res.getString(i + 1));
        if (i < columnCount - 1) {
          System.out.print(" | ");
        } else {
          System.out.print("\n");
        }
      }
    }
  }
}
