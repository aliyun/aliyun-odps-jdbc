import java.io.IOException;
import java.io.InputStream;
import java.lang.System;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import java.sql.Time;
import java.util.Properties;

public class DataDumpBenchmark {
  private static String driverName = "com.aliyun.odps.jdbc.OdpsDriver";

  /**
   * @param args
   * @throws SQLException
   */
  public static void main(String[] args) throws SQLException {
    try {
      Class.forName(driverName);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      System.exit(1);
    }

    int batchSize;

    try {
      batchSize = Integer.parseInt(args[0]);
    } catch (Exception e) {
      System.out.println(e);
      batchSize = 50000;
    }
    System.out.println("batch size: " + batchSize);

    // fill in the information string
    Properties odpsConfig = new Properties();
    InputStream is =
        Thread.currentThread().getContextClassLoader().getResourceAsStream("conf.properties");
    try {
      odpsConfig.load(is);
    } catch (IOException e) {
      e.printStackTrace();
      System.exit(1);
    }
    String accessId = odpsConfig.getProperty("access_id");
    String accessKey = odpsConfig.getProperty("access_key");
    Connection conn = DriverManager.getConnection(odpsConfig.getProperty("connection_string"), accessId, accessKey);

    Statement ddl = conn.createStatement();
    ddl.executeUpdate("drop table if exists m_instance_one_day;");
    ddl.executeUpdate(
        "create table m_instance_one_day(c1 string, c2 string, c3 string, c4 string, c5 string, c6 string, c7 bigint, c8 bigint, c9 bigint, c10 string);");

    PreparedStatement ps = conn.prepareStatement(
        "insert into m_instance_one_day values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");
    Statement query =  conn.createStatement();
    ResultSet rs = query.executeQuery("select * from meta.m_instance where ds = '20151120';");

    long start = System.currentTimeMillis();
    long now = start;

    int count = 0;
    while (rs.next()) {
      ps.setString(1, rs.getString(1));
      ps.setString(2, rs.getString(2));
      ps.setString(3, rs.getString(3));
      ps.setString(4, rs.getString(4));
      ps.setString(5, rs.getString(5));
      ps.setString(6, rs.getString(6));
      ps.setLong(7, rs.getLong(7));
      ps.setLong(8, rs.getLong(8));
      ps.setLong(9, rs.getLong(9));
      ps.setString(10, rs.getString(10));

      ps.addBatch();
      if(++count % batchSize == 0) {
        ps.executeBatch();
        long end = System.currentTimeMillis();
        System.out.printf("batch time: %.2f seconds\n", (float) (end - now) / 1000);
        now = end;
      }
    }

    ps.executeBatch(); // insert remaining records
    System.out.printf("total: %.2f minutes\n",
                      (float) (System.currentTimeMillis() - start) / 1000 / 60);

    ddl.close();
    ps.close();
    rs.close();
    query.close();
    conn.close();
  }
}
