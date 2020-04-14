import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

public class Pagination {

  private static void printPage(ResultSet res, int start, int count) throws SQLException {
    // 表头
    int columnCount = res.getMetaData().getColumnCount();
    for (int i = 0; i < columnCount; i++) {
      System.out.print(res.getMetaData().getColumnName(i + 1));
      if (i < columnCount - 1) {
        System.out.print(" | ");
      } else {
        System.out.print("\n");
      }
    }
    // 表内容
    res.absolute(start - 1);
    int c = 0;
    while (res.next()) {
      for (int i = 0; i < columnCount; i++) {
        System.out.print(res.getString(i + 1));
        if (i < columnCount - 1) {
          System.out.print(" | ");
        } else {
          System.out.print("\n");
        }
      }
      c++;
      if (c == count) {
        break;
      }
    }
  }

  public static void main(String[] args) throws SQLException {
    if (args.length < 3) {
      System.out.println("Usage: Pagination connection_string sql record_per_page");
      System.out.println(
          "   eg. Pagination 'jdbc:odps:http://service.odps.aliyun.com/api?project=odpsdemo?accessId=...&accessKey=...&charset=UTF-8' 'select * from dual' 10");
      System.exit(1);
    }
    String connectionString = args[0];
    String sql = args[1];
    int recordPerPage = Integer.parseInt(args[2]);

    try {
      String driverName = "com.aliyun.odps.jdbc.OdpsDriver";
      Class.forName(driverName);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      System.exit(1);
    }

    System.out.println("Connection: " + connectionString);
    Connection conn = DriverManager.getConnection(connectionString);
    ResultSet res;

    // SQL 只需运行一次，不要每显式一页都运行一个全新的 SQL
    System.out.println("Running   : " + sql);
    if (sql.trim().equalsIgnoreCase("show tables")) {
      res = conn.getMetaData().getTables(null, null, null, null);
    } else {
      Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
      res = stmt.executeQuery(sql);
    }

    // 获得 ResultSet 的记录数
    res.last();
    int recordCount = res.getRow();
    // 计算页数
    int pageCount = (int) Math.ceil(1.0 * recordCount / recordPerPage);

    while (true) {
      System.out.print("Page Count " + pageCount + ", please input page number (0 to exit): ");
      Scanner scanner = new Scanner(System.in);
      int p = scanner.nextInt();
      if (p == 0) {
        break;
      }
      // 根据输入的页号 p 显式页
      printPage(res, (p - 1) * recordPerPage + 1, recordPerPage);
    }

    // 退出分页时关闭 ResultSet 和 Connection
    res.close();
    conn.close();
  }
}
