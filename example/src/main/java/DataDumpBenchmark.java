import java.io.InputStream;
import java.lang.System;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class DataDumpBenchmark {

  private static String driverName = "com.aliyun.odps.jdbc.OdpsDriver";


  static String joinStrings(String[] s, String glue) {
    int k = s.length;
    if (k == 0) {
      return null;
    }
    StringBuilder out = new StringBuilder();
    out.append(s[0]);
    for (int x = 1; x < k; ++x) {
      out.append(glue).append(s[x]);
    }
    return out.toString();
  }

  /**
   * @param args
   * @throws SQLException
   */
  public static void main(String[] args) throws SQLException {

    final int batchSize;
    final int splitNum;
    final Properties odpsConfig = new Properties();
    try {
      splitNum = Integer.parseInt(args[0]);
      batchSize = Integer.parseInt(args[1]);
      System.out.println("split size: " + batchSize);
      System.out.println("batch size: " + splitNum);

      Class.forName(driverName);

      InputStream is =
          Thread.currentThread().getContextClassLoader().getResourceAsStream("conf.properties");
      odpsConfig.load(is);

    } catch (Exception e) {
      System.out.println(e);
      return;
    }

    final Connection connSource = DriverManager.getConnection(
        "jdbc:odps:" + odpsConfig.getProperty("endpoint_1") + "?project=meta_dev&loglevel=debug&",
        odpsConfig.getProperty("access_id_1"),
        odpsConfig.getProperty("access_key_1"));

    final Connection connTarget = DriverManager.getConnection(
        "jdbc:odps:" + odpsConfig.getProperty("endpoint_2") + "?project=odps_test_sqltask_finance&loglevel=debug",
        odpsConfig.getProperty("access_id_2"),
        odpsConfig.getProperty("access_key_2"));

    String sourceSchema = "meta";
    String sourceTable = "m_instance";
    String targetTable = "m_instance_copy";
    String sourceWhere = "ds = '20151120'";
    String splitColumn = "start_time";

    ResultSet cols = connSource.getMetaData().getColumns(null, sourceSchema, sourceTable, null);
    List<String> nameTypePairs = new ArrayList<String>();
    List<String> questionMarks = new ArrayList<String>();
    while (cols.next()) {
      nameTypePairs.add(cols.getString("COLUMN_NAME") + " " + cols.getString("TYPE_NAME"));
      questionMarks.add("?");
    }

    final int colNums = nameTypePairs.size();
    if (sourceSchema != null) {
      sourceTable = sourceSchema + "." + sourceTable;
    }

    String createTableSql = "create table " + targetTable + "(" +
                            joinStrings(nameTypePairs.toArray(new String[nameTypePairs.size()]),
                                        ", ") + ")";
    final String insertValuesSql = "insert into " + targetTable + " values (" +
                             joinStrings(questionMarks.toArray(new String[questionMarks.size()]),
                                         ", ") + ")";

    String rangeSql = "select max(" + splitColumn + "), min(" + splitColumn + ") from " + sourceTable;
    if (sourceWhere != null) {
      rangeSql += " where " + sourceWhere;
    }

    Statement ddl = connTarget.createStatement();
    ddl.executeUpdate("drop table if exists " + targetTable);
    ddl.executeUpdate(createTableSql);
    ddl.close();

    Statement query = connSource.createStatement();
    ResultSet range = query.executeQuery(rangeSql);
    long max, min;
    range.next();
    max = range.getLong(1);
    min = range.getLong(2);
    range.close();
    query.close();

    long[] markers = new long[splitNum + 1];
    markers[0] = min;
    for (int i = 1; i < splitNum; i++) {
      markers[i] = markers[i - 1] + (max - min + 1) / splitNum;
    }
    markers[splitNum] = max;

    String[] cutters = new String[splitNum];
    for (int i = 0; i < splitNum - 1; i++) {
      cutters[i] = markers[i] + " <= " + splitColumn + " and " + splitColumn + " < " + markers[i + 1];
    }
    cutters[splitNum - 1] =
        markers[splitNum - 1] + " <= " + splitColumn + " and " + splitColumn + " <= " + markers[splitNum];

    connSource.close();
    connTarget.close();


    long start = System.currentTimeMillis();


    ArrayList<Callable<Long>> callList = new ArrayList<Callable<Long>>();
    for (int i = 0; i < splitNum; i++) {

      String selectSql = "select * from " + sourceTable + " where ";
      selectSql += cutters[i];
      if (sourceWhere != null) {
        selectSql += " and " + sourceWhere;
      }

      final String selectSql2 = selectSql;
      Callable<Long> call = new Callable<Long>() {
        public Long call() throws Exception {
          final Connection connSource = DriverManager.getConnection(
              "jdbc:odps:" + odpsConfig.getProperty("endpoint_1")
              + "?project=meta_dev&loglevel=debug&",
              odpsConfig.getProperty("access_id_1"),
              odpsConfig.getProperty("access_key_1"));

          final Connection connTarget = DriverManager.getConnection(
              "jdbc:odps:" + odpsConfig.getProperty("endpoint_2")
              + "?project=odps_test_sqltask_finance&loglevel=debug",
              odpsConfig.getProperty("access_id_2"),
              odpsConfig.getProperty("access_key_2"));

          Statement query = connSource.createStatement();
          PreparedStatement insert = connTarget.prepareStatement(insertValuesSql);
          ResultSet rs = query.executeQuery(selectSql2);
          long start = System.currentTimeMillis();
          long now = start;

          int count = 0;
          while (rs.next()) {
            for (int i = 0; i < colNums; i++) {
              try {
                insert.setObject((i + 1), rs.getObject(i + 1));
              } catch (SQLException e) {
                e.printStackTrace();
                System.exit(1);
              }
            }

            insert.addBatch();
            if (++count % batchSize == 0) {
              insert.executeBatch();
              long end = System.currentTimeMillis();
              System.out.printf("batch time: %.2f seconds\n", (float) (end - now) / 1000);
              now = end;
            }
          }

          insert.executeBatch(); // insert remaining records
          rs.close();
          query.close();
          insert.close();
          connSource.close();
          connTarget.close();
          return 0L;
        }
      };
      callList.add(call);
    }

    ExecutorService executors = Executors.newFixedThreadPool(2);
    try {
      executors.invokeAll(callList);
    } catch (InterruptedException e) {
      e.printStackTrace();
      return;
    }
    executors.shutdown();

    System.out.printf("total: %.2f minutes\n",
                      (float) (System.currentTimeMillis() - start) / 1000 / 60);
  }
}
