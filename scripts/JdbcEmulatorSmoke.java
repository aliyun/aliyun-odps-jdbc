import java.math.BigDecimal;
import java.net.URI;
import java.sql.*;
import java.util.Arrays;
import java.util.Properties;

/** SQL and Tunnel acceptance against a local emulator using only the shaded candidate jar. */
public final class JdbcEmulatorSmoke {
  private static void check(boolean value, String message) {
    if (!value) throw new AssertionError(message);
  }

  private static Connection connect(String endpoint, String mode) throws Exception {
    Properties p = new Properties();
    p.setProperty("access_id", "test-ak");
    p.setProperty("access_key", "test-sk");
    p.setProperty("project_name", "test_project");
    p.setProperty("tunnelEndpoint", endpoint);
    p.setProperty("enableLimit", "false");
    p.setProperty("log_level", "OFF");
    p.setProperty("interactiveMode", mode);
    return DriverManager.getConnection("jdbc:odps:" + endpoint, p);
  }

  public static void main(String[] args) throws Exception {
    String endpoint = args[0];
    String host = URI.create(endpoint).getHost();
    check("127.0.0.1".equals(host) || "localhost".equals(host), "Emulator must be on loopback");
    Class.forName("com.aliyun.odps.jdbc.OdpsDriver");
    String table = "jdbc_emulator_" + Long.toHexString(System.nanoTime());
    try (Connection c = connect(endpoint, "false"); Statement s = c.createStatement()) {
      s.execute("set odps.sql.type.system.odps2=true");
      try (PreparedStatement p = c.prepareStatement("select ? as y, ? as s, ? as n, ? as d, ? as f")) {
        p.setByte(1, Byte.MIN_VALUE); p.setShort(2, Short.MAX_VALUE);
        p.setLong(3, Long.MIN_VALUE); p.setBigDecimal(4, new BigDecimal("123.450"));
        p.setFloat(5, 1.25f);
        try (ResultSet r = p.executeQuery()) {
          check(r.next(), "Missing typed literal row");
          check(r.getByte(1) == Byte.MIN_VALUE && r.getShort(2) == Short.MAX_VALUE, "Narrow literal value");
          check(r.getLong(3) == Long.MIN_VALUE, "BIGINT boundary");
          check(r.getBigDecimal(4).compareTo(new BigDecimal("123.450")) == 0, "Decimal precision");
          check(r.getFloat(5) == 1.25f, "FLOAT binding");
          check(r.getMetaData().getColumnType(1) == Types.TINYINT, "TINYINT metadata");
          check(r.getMetaData().getColumnType(2) == Types.SMALLINT, "SMALLINT metadata");
          check(r.getMetaData().getColumnType(3) == Types.BIGINT, "BIGINT metadata");
          check(!r.next(), "Extra literal row");
        }
      }
      System.out.println("PASS typed PreparedStatement literals, bounds, precision and metadata");
      s.execute("create table " + table + " (id bigint, text string, a array<int>, person struct<name:string,age:int>)");
      try {
        try (PreparedStatement p = c.prepareStatement("insert into " + table + " values (?, ?, ?, ?)")) {
          for (int i = 1; i <= 3; i++) {
            p.setLong(1, i); p.setString(2, "row-'" + i);
            p.setArray(3, c.createArrayOf("INT", new Object[] {i, i + 1}));
            p.setObject(4, c.createStruct("struct<name:string,age:int>", new Object[] {"person" + i, 20 + i}));
            p.addBatch();
          }
          check(p.executeBatch().length == 3, "Batch upload count");
        }
        try (ResultSet r = s.executeQuery("select * from " + table + " order by id")) {
          for (int i = 1; i <= 3; i++) {
            check(r.next() && r.getLong(1) == i, "Tunnel row order");
            check(("row-'" + i).equals(r.getString(2)), "Quoted string round trip");
            check(Arrays.equals(new Object[] {i, i + 1}, (Object[]) r.getArray(3).getArray()), "ARRAY round trip");
            check(r.getString(4).contains("person" + i), "STRUCT round trip");
          }
          check(!r.next(), "Duplicate uploaded rows");
        }
        System.out.println("PASS batch Tunnel upload/download, quoted strings, ARRAY and STRUCT");
        s.setMaxRows(2);
        try (ResultSet r = s.executeQuery("select id from " + table + " order by id")) {
          int n = 0; while (r.next()) n++;
          check(n == 2, "setMaxRows must bound the result");
        }
        s.setMaxRows(0);
        try (ResultSet r = c.getMetaData().getColumns("test_project", null, table, "%")) {
          int n = 0; while (r.next()) n++;
          check(n == 4, "Catalog columns");
        }
        System.out.println("PASS JDBC row limits and catalog metadata");
        try {
          s.executeQuery("select * from jdbc_emulator_missing_table");
          throw new AssertionError("Missing table must fail");
        } catch (SQLException expected) { /* A protocol error must reach JDBC. */ }
        try (ResultSet r = s.executeQuery("select cast(42 as bigint)")) {
          check(r.next() && r.getLong(1) == 42, "Connection recovery after SQL error");
        }
        System.out.println("PASS SQL error propagation and subsequent connection reuse");
      } finally { s.execute("drop table if exists " + table); }
    }
    try (Connection c = connect(endpoint, "mcqa"); Statement s = c.createStatement();
         ResultSet r = s.executeQuery("select 73L as n, array(1,2) as a")) {
      check(r.next() && r.getLong(1) == 73, "MCQA SQLRT result");
      check(r.getArray(2).getArray() != null && !r.next(), "MCQA complex result");
    }
    System.out.println("PASS MCQA SQLRT session, subquery and Tunnel result consumption");
  }
}
