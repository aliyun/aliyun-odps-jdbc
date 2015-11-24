import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import java.sql.Time;

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

    // fill in the information here
    String accessId = "";
    String accessKey = "";
    Connection conn = DriverManager.getConnection("jdbc:odps:http://service-corp.odps.aliyun-inc.com/api?project=meta_dev&loglevel=debug", accessId, accessKey);


    Statement ddl = conn.createStatement();
    ddl.executeUpdate("drop table if exists m_instance_one_day;");
    ddl.executeUpdate("create table m_instance_one_day(c1 string, c2 string, c3 string, c4 string, c5 string, c6 string, c7 bigint, c8 bigint, c9 bigint, c10 string);");
    ddl.close();

    PreparedStatement ps = conn.prepareStatement(
        "insert into m_instance_one_day values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");

    final int batchSize = 50000;
    int count = 0;

    Statement query =  conn.createStatement();
    ResultSet rs = query.executeQuery("select * from meta.m_instance where ds = '20151120';");
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
      }
    }

    ps.executeBatch(); // insert remaining records
    ps.close();
    rs.close();
    query.close();
    conn.close();
  }
}
