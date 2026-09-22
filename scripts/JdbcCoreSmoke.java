import java.sql.*;
import java.util.Properties;

/** Live, read-only acceptance of the packaged JDBC driver; failures never skip. */
public final class JdbcCoreSmoke {
  private static String required(String name) {
    String value = System.getenv(name);
    if (value == null || value.trim().isEmpty()) {
      throw new IllegalStateException("Missing environment variable: " + name);
    }
    return value;
  }

  private static void check(boolean condition, String message) {
    if (!condition) throw new AssertionError(message);
  }

  public static void main(String[] args) throws Exception {
    Properties properties = new Properties();
    properties.setProperty("access_id", required("ALIBABA_CLOUD_ACCESS_KEY_ID"));
    properties.setProperty("access_key", required("ALIBABA_CLOUD_ACCESS_KEY_SECRET"));
    properties.setProperty("project_name", required("MAXCOMPUTE_PROJECT"));
    properties.setProperty("log_level", "OFF");
    String token = System.getenv("ALIBABA_CLOUD_SECURITY_TOKEN");
    if (token != null && !token.trim().isEmpty()) properties.setProperty("sts_token", token);
    String endpoint = required("MAXCOMPUTE_ENDPOINT");
    Class.forName("com.aliyun.odps.jdbc.OdpsDriver");
    try (Connection connection = DriverManager.getConnection("jdbc:odps:" + endpoint, properties)) {
      check(!connection.isClosed(), "Connection is closed");
      try (Statement statement = connection.createStatement()) {
        try (ResultSet rows = statement.executeQuery(
            "select cast(42 as bigint) as n, 'jdbc-core' as s, cast(null as string) as absent")) {
          check(rows.getMetaData().getColumnCount() == 3, "Wrong column count");
          check(rows.next(), "No result row");
          check(rows.getLong(1) == 42, "Wrong numeric result");
          check("jdbc-core".equals(rows.getString(2)), "Wrong string result");
          check(rows.getString(3) == null && rows.wasNull(), "Wrong NULL result");
          check(!rows.next(), "Unexpected extra row");
        }
      }
      System.out.println("PASS Statement.executeQuery: numeric/string/NULL values and metadata");
      try (PreparedStatement statement = connection.prepareStatement(
          "select cast(? as bigint) as n, ? as s")) {
        statement.setLong(1, 73);
        statement.setString(2, "jdbc-parameter");
        try (ResultSet rows = statement.executeQuery()) {
          check(rows.next(), "No prepared result row");
          check(rows.getLong(1) == 73, "Wrong bound numeric result");
          check("jdbc-parameter".equals(rows.getString(2)), "Wrong bound string result");
          check(!rows.next(), "Unexpected prepared result row");
        }
      }
      System.out.println("PASS PreparedStatement: parameter binding and result consumption");
    } catch (SQLException failure) {
      // Do not print connection properties, URLs, or server messages with credentials.
      System.err.println("FAIL JDBC core SQL test: SQLState=" + failure.getSQLState()
          + ", errorCode=" + failure.getErrorCode());
      System.exit(1);
    }
    for (String legacy : new String[] {"default", "true", "false"}) {
      Properties arrayProperties = new Properties();
      arrayProperties.putAll(properties);
      if (!"default".equals(legacy)) arrayProperties.setProperty("legacy_array_get_object", legacy);
      try (Connection connection = DriverManager.getConnection("jdbc:odps:" + endpoint, arrayProperties);
           Statement statement = connection.createStatement();
           ResultSet rows = statement.executeQuery("select array(1, 2) as a, array('x', 'y') as b")) {
        check(rows.getMetaData().getColumnType(1) == Types.ARRAY, "Wrong ARRAY metadata");
        check(rows.next(), "No ARRAY row");
        boolean standard = "false".equals(legacy);
        check(standard ? rows.getObject(1) instanceof Array : rows.getObject(1) instanceof java.util.List,
            "Wrong untyped ARRAY mapping: " + legacy);
        check(standard ? rows.getObject("b") instanceof Array : rows.getObject("b") instanceof java.util.List,
            "Wrong label ARRAY mapping: " + legacy);
        check("[1, 2]".equals(rows.getObject(1, java.util.List.class).toString()), "Wrong typed List");
        check("[x, y]".equals(rows.getObject("b", java.util.List.class).toString()), "Wrong label typed List");
        check("[1, 2]".equals(java.util.Arrays.toString((Object[]) rows.getObject(1, Array.class).getArray())),
            "Wrong typed Array");
        check("[x, y]".equals(java.util.Arrays.toString((Object[]) rows.getObject("b", Array.class).getArray())),
            "Wrong label typed Array");
        check("[1, 2]".equals(java.util.Arrays.toString((Object[]) rows.getArray(1).getArray())), "Wrong getArray");
        check(!rows.next(), "Unexpected extra ARRAY row");
      } catch (SQLException failure) {
        System.err.println("FAIL JDBC ARRAY test: mode=" + legacy + ", SQLState=" + failure.getSQLState()
            + ", errorCode=" + failure.getErrorCode());
        System.exit(1);
      }
      System.out.println("PASS ARRAY mapping and typed getters: legacy=" + legacy);
    }
    System.out.println("PASS JDBC core acceptance: connection, real SQL, results and resource closure");
  }
}
