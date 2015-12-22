import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import com.zaxxer.hikari.HikariDataSource;


public class HikariCP {

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


    // fill in the information string
    Properties odpsConfig = new Properties();
    InputStream
        is =
        Thread.currentThread().getContextClassLoader().getResourceAsStream("conf.properties");
    try {
      odpsConfig.load(is);
    } catch (IOException e) {
      e.printStackTrace();
      System.exit(1);
    }


    try {

      HikariDataSource ds = new HikariDataSource();
      ds.setDriverClassName(driverName);
      ds.setJdbcUrl(odpsConfig.getProperty("connection_string"));
      ds.setUsername(odpsConfig.getProperty("username"));
      ds.setPassword(odpsConfig.getProperty("password"));
      ds.setMaximumPoolSize(5);
      ds.setAutoCommit(false);
      ds.setReadOnly(false);


      Connection conn = ds.getConnection();
      Statement stmt = conn.createStatement();
      String tableName = "testOdpsDriverTable";
      stmt.execute("drop table if exists " + tableName);
      stmt.execute("create table " + tableName + " (key int, value string)");

      String sql;
      ResultSet res;

      // insert a record
      sql =
          String.format(
              "insert into table %s select 24 key, 'hours' value from (select count(1) from %s) a",
              tableName, tableName);
      System.out.println("Running: " + sql);
      int count = stmt.executeUpdate(sql);
      System.out.println("updated records: " + count);

      // select * query
      sql = "select * from " + tableName;
      System.out.println("Running: " + sql);
      res = stmt.executeQuery(sql);
      while (res.next()) {
        System.out.println(String.valueOf(res.getInt(1)) + "\t" + res.getString(2));
      }

      // regular query
      sql = "select count(1) from " + tableName;
      System.out.println("Running: " + sql);
      res = stmt.executeQuery(sql);
      while (res.next()) {
        System.out.println(res.getString(1));
      }

      ds.close();

    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
  }
}
