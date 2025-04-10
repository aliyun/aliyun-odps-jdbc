package com.aliyun.odps.jdbc;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.odps.OdpsException;

public class JdbcRunner {

  public static void main(String[] args) throws SQLException, OdpsException {
    if (args.length < 2) {
      System.err.println(
          "Usage: java -cp odps-jdbc-version-jar-with-dependencies.jar com.aliyun.odps.jdbc.JdbcRunner <jdbc_url> <sql_file>");
      System.exit(1);
    }

    String jdbcUrl = args[0];
    String sqlFilePath = args[1];
    String sqlContent = readFile(sqlFilePath);

    System.out.println("JDBC URL: " + jdbcUrl);

    OdpsConnection conn = (OdpsConnection) DriverManager.getConnection(jdbcUrl);
    OdpsStatement stmt = conn.createStatement();
    if (sqlContent == null) {
      System.out.println("no sql content.");
      System.exit(0);
    }

    long start = System.currentTimeMillis();
    stmt.execute(sqlContent);
    System.out.println(stmt.getSqlExecutor().getLogView());
    OdpsResultSet odpsResultSet = (OdpsResultSet) stmt.getResultSet();

    long cost = System.currentTimeMillis() - start;

    List<String> columnNames = new ArrayList<>();
    for (int i = 1; i <= odpsResultSet.getMetaData().getColumnCount(); i++) {
      columnNames.add(odpsResultSet.getMetaData().getColumnName(i));
    }

    System.out.println(columnNames.stream().collect(Collectors.joining("\t")));
    while (odpsResultSet.next()) {
      StringBuilder sb = new StringBuilder();
      for (int i = 1; i <= columnNames.size(); i++) {
        Object object = odpsResultSet.getObject(i);
        sb.append(object == null ? "NULL" : object.toString());
        if (i != columnNames.size()) {
          sb.append("\t");
        }
      }
      System.out.println(sb);
    }

    System.out.println("Summary:\n" + stmt.getSqlExecutor().getSummary());
    System.out.println("Execute cost " + cost + " ms;");
    System.exit(0);
  }

  private static String readFile(String filePath) {
    StringBuilder contentBuilder = new StringBuilder();
    try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
      String line;
      while ((line = br.readLine()) != null) {
        contentBuilder.append(line).append("\n");
      }
    } catch (IOException e) {
      System.err.println("Error reading file: " + e.getMessage());
      return null;
    }
    return contentBuilder.toString();
  }
}
