package com.aliyun.odps.jdbc;


import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

/**
 * This class manage a global JDBC connection and multiple testing instances
 * can access it simultaneously. It will also close the connection automatically.
 */
public class OdpsConnectionFactory {

  public Connection conn;

  private static final OdpsConnectionFactory cf = new OdpsConnectionFactory();

  private OdpsConnectionFactory() {
    try {
      Properties odpsConfig = new Properties();

      InputStream is =
          Thread.currentThread().getContextClassLoader().getResourceAsStream("bvt_conf.properties");
      odpsConfig.load(is);

      Class.forName("com.aliyun.odps.jdbc.OdpsDriver");
      String url = odpsConfig.getProperty("end_point");
      conn = DriverManager.getConnection("jdbc:odps:" + url, odpsConfig);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    } catch (java.sql.SQLException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static OdpsConnectionFactory getInstance() {
    return cf;
  }

  @Override
  protected void finalize() throws Throwable {
    conn.close();
  }
}

