import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class OdpsJdbcClient {
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
    String accessId = "your_access_id";
    String accessKey = "your_access_key";
    Connection con = DriverManager.getConnection("jdbc:odps:https://service-corp.odps.aliyun-inc.com/api?project=<your_project_name>", accessId, accessKey);
    Statement stmt = con.createStatement();
    String tableName = "testOdpsDriverTable";
    stmt.execute("drop table if exists " + tableName);
    stmt.execute("create table " + tableName + " (key int, value string)");

    String sql;
    ResultSet res;

    // insert a record
    sql = String.format("insert into table %s select 24 key, 'hours' value from (select count(1) from %s) a", tableName, tableName);
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
  }
}
