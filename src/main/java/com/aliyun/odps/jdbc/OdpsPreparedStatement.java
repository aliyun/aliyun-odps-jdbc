/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */

package com.aliyun.odps.jdbc;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.Binary;
import com.aliyun.odps.data.Char;
import com.aliyun.odps.data.Varchar;
import com.aliyun.odps.jdbc.utils.JdbcColumn;
import com.aliyun.odps.sqa.commandapi.utils.SqlParserUtil;
import com.aliyun.odps.tunnel.TunnelException;

public class OdpsPreparedStatement extends AbstractOdpsPreparedStatement {

  private final String TABLE_NAME = "((\\w+\\.)?\\w+)";      // "proj.name" or "name"
  private final String PREP_VALUES = "\\((\\s*\\?\\s*)(,\\s*\\?\\s*)*\\)"; // "(?)" or "(?,?,...)"
  private final String SPEC_COLUMN = "(\\([^()]*\\))?"; // "" or "(a,b,c)"

  private final String
      SPEC_PARTITION =
      "\\((\\s*\\w+\\s*=\\s*(\\w+|'\\w+')(\\s*,\\s*\\w+\\s*=\\s*(\\w+|'\\w+')\\s*)*\\s*)\\)";
  // (p1=a) or (p1=a,p2=b,...) or (p1='a') ...


  private final String PREP_INSERT_WITH_SPEC_PARTITION =
      "(?i)^" + "\\s*" + "insert" + "\\s+" + "into" + "\\s+" + TABLE_NAME + "\\s*" + SPEC_COLUMN
      + "\\s+" + "partition" + SPEC_PARTITION + "\\s+" + "values" + "\\s*" + PREP_VALUES + "\\s*"
      + ";?\\s*$";

  private final String EXAMPLE =
      "INSERT INTO table [(c1, c2)] [partition(p1=a,p2=b,...)] VALUES (?, ?);";

  private final String PREP_INSERT_WITHOUT_SPEC_PARTITION =
      "(?i)^" + "\\s*" + "insert" + "\\s+" + "into" + "\\s+" + TABLE_NAME + "\\s*" + SPEC_COLUMN
      + "\\s+" + "values" + "\\s*" + PREP_VALUES + "\\s*" + ";?\\s*$";

  private static final String
      SQL_REGEX =
      "(')|(--)|(/\\*(?:.|[\\n\\r])*?\\*/)|(\\b(select|update|and|or|delete|insert|trancate|char|substr|ascii|declare|exec|count|master|into|drop|execute)\\b)";

  private static final Pattern SQL_PATTERN = Pattern.compile(SQL_REGEX, Pattern.CASE_INSENSITIVE);


  static ThreadLocal<SimpleDateFormat>
      DATETIME_FORMAT =
      ThreadLocal.withInitial(() -> new SimpleDateFormat(JdbcColumn.ODPS_DATETIME_FORMAT));
  static ThreadLocal<SimpleDateFormat>
      DATE_FORMAT =
      ThreadLocal.withInitial(() -> new SimpleDateFormat(JdbcColumn.ODPS_DATE_FORMAT));

  static ThreadLocal<DateTimeFormatter>
      LOCAL_DATE_FORMAT =
      ThreadLocal.withInitial(() -> DateTimeFormatter.ofPattern(JdbcColumn.ODPS_DATE_FORMAT)
          .withZone(ZoneId.systemDefault()));

  static ThreadLocal<DateTimeFormatter>
      ZONED_DATETIME_FORMAT =
      ThreadLocal.withInitial(() -> DateTimeFormatter.ofPattern(JdbcColumn.ODPS_DATETIME_FORMAT)
          .withZone(ZoneId.systemDefault()));

  static ThreadLocal<DateTimeFormatter>
      ZONED_TIMESTAMP_FORMAT =
      ThreadLocal.withInitial(() -> DateTimeFormatter.ofPattern(JdbcColumn.ODPS_TIMESTAMP_FORMAT)
          .withZone(ZoneId.systemDefault()));


  /**
   * The prepared sql template (immutable). e.g. insert into table FOO select * from BAR where id =
   * ? and weight = ?
   */
  private final String sql;

  private boolean parsed = false;
  private String tableBatchInsertTo;

  private String projectName;
  private String schemaName;
  private String tableName;
  private String partitionSpec;
  private List<String> specificColumns;

  private int parametersNum;

  private DataUploader uploader;

  /**
   * The parameters for the prepared sql (index=>parameter). The parameter is stored as Java objects
   * and lazily casted into String when submitting sql. The executeBatch() call will utilize it to
   * upload data to ODPS via tunnel.
   */
  private HashMap<Integer, Object> parameters = new HashMap<>();

  // When addBatch(), compress the parameters into a row
  private List<Object[]> batchedRows = new ArrayList<>();

  OdpsPreparedStatement(OdpsConnection conn, String sql) {
    super(conn);
    this.sql = sql;

    int counter = 0;
    for (int i = 0; i < sql.length(); i++) {
      if (sql.charAt(i) == '?') {
        counter++;
      }
    }
    this.parametersNum = counter;

    conn.log.info("create prepared statements: " + sql);
  }

  OdpsPreparedStatement(OdpsConnection conn, String sql, boolean isResultSetScrollable) {
    this(conn, sql);
    this.isResultSetScrollable = isResultSetScrollable;
  }

  @Override
  public void addBatch() throws SQLException {
    Object[] arr = new Object[parametersNum];
    for (int i = 0; i < arr.length; i++) {
      arr[i] = parameters.get(i + 1);
    }
    batchedRows.add(arr);
    parameters.clear();
  }

  @Override
  public void clearParameters() throws SQLException {
    parameters.clear();
  }

  @Override
  public void clearBatch() throws SQLException {
    batchedRows.clear();
  }

  /**
   * Only support DML like `INSERT INTO table_name values (e, f, g)` in batch execution
   * <p>
   * Since ODPS SQL does not provide this functionality, we 1) hijack such kind of batched SQLs , 2)
   * assemble the records by ourselves, and 3) call tunnel API to upload them.
   * <p>
   * We verify and parse the SQL to extract table name in the first call to executeBatch(), so that
   * other kinds of statement can be executed in a non-batch way.
   * <p>
   * Considering performance issue, We check it lazily in executeBatch() instead of addBatch().
   *
   * @throws SQLException when 1) wrong syntax 2) columns not match
   */
  @Override
  public int[] executeBatch() throws SQLException {
    if (!parsed) {
      parse();
    }

    int[] updateCounts = uploader.upload(batchedRows);
    clearBatch();
    return updateCounts;
  }

  private void parse() throws SQLException {

    boolean withSpecPartition = sql.matches(PREP_INSERT_WITH_SPEC_PARTITION);
    boolean withoutSpecPartition = sql.matches(PREP_INSERT_WITHOUT_SPEC_PARTITION);

    if (!withoutSpecPartition && !withSpecPartition) {
      throw new SQLException("batched statement only support following syntax: " + EXAMPLE);
    }

    Matcher matcher = null;
    boolean hasPartition = false;

    if (withoutSpecPartition) {
      matcher = Pattern.compile(PREP_INSERT_WITHOUT_SPEC_PARTITION).matcher(sql);
      hasPartition = false;
    }

    if (withSpecPartition) {
      matcher = Pattern.compile(PREP_INSERT_WITH_SPEC_PARTITION).matcher(sql);
      hasPartition = true;
    }


    if (matcher.find()) {
      tableBatchInsertTo = matcher.group(1);
      if (hasPartition) {
        partitionSpec = matcher.group(4);
      }
      if (tableBatchInsertTo.contains(".")) {
        String[] splited = tableBatchInsertTo.split("\\.");
        projectName = splited[0];
        tableName = splited[1];
      } else {
        projectName = getConnection().getOdps().getDefaultProject();
        tableName = tableBatchInsertTo;
      }
    } else {
      throw new SQLException("cannot extract table name or partition name in SQL: " + sql);
    }
    List<String> specificColumns =
        Optional.ofNullable(matcher.group(3)).map(s -> s.substring(1, s.length() - 1))
            .map(s -> s.split(","))
            .map(s -> Arrays.stream(s).map(String::trim).collect(Collectors.toList())).orElse(null);
    if (specificColumns != null) {
      if (specificColumns.size() != batchedRows.get(0).length) {
        throw new SQLException(
            "sql has specific " + specificColumns + " columns, but only prepare " + batchedRows.get(
                0).length + " values");
      }
      this.specificColumns = specificColumns;
    }

    try {
      uploader = DataUploader.build(projectName, schemaName, tableName, partitionSpec,
                                    specificColumns, getConnection());
    } catch (OdpsException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new SQLException(e);
    }

    parsed = true;
  }

  // Commit on close
  @Override
  public void close() throws SQLException {
    if (isClosed()) {
      return;
    }

    if (uploader != null) {
      try {
        uploader.commit();
      } catch (TunnelException | IOException e) {
        throw new SQLException(e);
      }
    }
    super.close();
  }

  /**
   * 解析后通过SQLExecutor作为query执行
   * 这种方式执行写入时间数据的时候，1900年前的时间可能会触发Java的时区问题，因此建议采用executeUpdate()方式
   * Java时区问题：https://programminghints.com/2017/05/still-using-java-util-date-dont/
   * https://stackoverflow.com/questions/41723123/java-timezone-what-transitions-mean
   * @return
   * @throws SQLException
   */
  @Override
  public boolean execute() throws SQLException {
    return super.execute(updateSql(sql, parameters));
  }

  /**
   * 解析后通过SQLExecutor作为query执行，同execute()
   * @return
   * @throws SQLException
   */
  @Override
  public ResultSet executeQuery() throws SQLException {
    return super.executeQuery(updateSql(sql, parameters));
  }

  /**
   * 解析后通过table tunnel执行，只支持insert命令
   * 该方式采用java.time类型写入时间数据的时候，可以避免Java的时区问题
   * Java时区问题：https://programminghints.com/2017/05/still-using-java-util-date-dont/
   * https://stackoverflow.com/questions/41723123/java-timezone-what-transitions-mean
   * @return
   * @throws SQLException
   */
  @Override
  public int executeUpdate() throws SQLException {
    addBatch();
    return executeBatch().length;
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    if (getExecuteInstance() == null) {
      this.execute();
    }
    return getResultSet().getMetaData();
  }

  @Override
  public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    parameters.put(parameterIndex, x);
  }

  @Override
  public void setNull(int parameterIndex, int sqlType, String typeName)
      throws SQLException {
    // ODPS doesn't care the type of NULL. So the second parameter is simply ignored.
    parameters.put(parameterIndex, null);
  }

  /**
   * 1. 对于prepareStatement本身支持的类型，setObject直接调用相关的接口
   * 2. 对于prepareStatement本身不支持的类型，例如java.util.Date, java.time, odps类型，直接保留类型进行透传
   *
   * @param parameterIndex the first parameter is 1, the second is 2, ...
   * @param x              the object containing the input parameter value
   * @throws SQLException
   */
  @Override
  public void setObject(int parameterIndex, Object x) throws SQLException {
    if (x == null) {
      setNull(parameterIndex, Types.NULL);
    } else if (x instanceof String) {
      setString(parameterIndex, (String) x);
    } else if (x instanceof byte[]) {
      setBytes(parameterIndex, (byte[]) x);
    } else if (x instanceof Short) {
      setShort(parameterIndex, (Short) x);
    } else if (x instanceof Integer) {
      setInt(parameterIndex, (Integer) x);
    } else if (x instanceof Long) {
      setLong(parameterIndex, (Long) x);
    } else if (x instanceof Float) {
      setFloat(parameterIndex, (Float) x);
    } else if (x instanceof Double) {
      setDouble(parameterIndex, (Double) x);
    } else if (x instanceof Boolean) {
      setBoolean(parameterIndex, (Boolean) x);
    } else if (x instanceof Byte) {
      setByte(parameterIndex, (Byte) x);
    } else if (x instanceof BigDecimal) {
      setBigDecimal(parameterIndex, (BigDecimal) x);
    } else if (x instanceof Timestamp) {
      setTimestamp(parameterIndex, (Timestamp) x);
    } else if (x instanceof Time) {
      setTime(parameterIndex, (Time) x);
    } else if (x instanceof Date) {
      setDate(parameterIndex, (Date) x);
    } else if (x instanceof java.util.Date) {
      parameters.put(parameterIndex, x);
    } else if (x instanceof LocalDate) {
      parameters.put(parameterIndex, x);
    } else if (x instanceof ZonedDateTime) {
      parameters.put(parameterIndex, x);
    } else if (x instanceof Instant) {
      parameters.put(parameterIndex, x);
    } else if (x instanceof Varchar) {
      setString(parameterIndex, x.toString());
    } else if (x instanceof Char) {
      setString(parameterIndex, x.toString());
    } else if (x instanceof Binary) {
      parameters.put(parameterIndex, x);
    } else {
      throw new SQLException("can not set an object of type: " + x.getClass().getName());
    }
  }

  @Override
  public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
    parameters.put(parameterIndex, x);
  }

  @Override
  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    parameters.put(parameterIndex, x);
  }

  @Override
  public void setByte(int parameterIndex, byte x) throws SQLException {
    parameters.put(parameterIndex, x);
  }

  public void setDate(int parameterIndex, Date x) throws SQLException {
    parameters.put(parameterIndex, x);
  }

  @Override
  public void setDouble(int parameterIndex, double x) throws SQLException {
    parameters.put(parameterIndex, x);
  }

  @Override
  public void setFloat(int parameterIndex, float x) throws SQLException {
    parameters.put(parameterIndex, x);
  }

  @Override
  public void setInt(int parameterIndex, int x) throws SQLException {
    parameters.put(parameterIndex, x);
  }

  @Override
  public void setLong(int parameterIndex, long x) throws SQLException {
    parameters.put(parameterIndex, x);
  }

  @Override
  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    // ODPS doesn't care the type of NULL. So the second parameter is simply ignored.
    parameters.put(parameterIndex, null);
  }

  @Override
  public void setShort(int parameterIndex, short x) throws SQLException {
    parameters.put(parameterIndex, x);
  }

  @Override
  public void setString(int parameterIndex, String x) throws SQLException {
    if (x == null) {
      parameters.put(parameterIndex, null);
      return;
    }
    parameters.put(parameterIndex, x.getBytes());
  }

  @Override
  public void setTime(int parameterIndex, Time x) throws SQLException {
    parameters.put(parameterIndex, x);
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
    parameters.put(parameterIndex, x);
  }

  /**
   * Returns a new sql replacing the '?'s in the prepared sql to parameters.
   */
  private String updateSql(String sql, HashMap<Integer, Object> parameters) throws SQLException {
    List<Integer> indexList = SqlParserUtil.getPlaceholderIndexList(sql);
    if (indexList.size() == 0) {
      return sql;
    }

    if (parameters == null || parameters.size() != indexList.size()) {
      throw new SQLException("wrong number of parameters.");
    }

    StringBuilder newSql = new StringBuilder(sql);
    int pos = 0;
    int paramIndex = 1;

    for (Integer index : indexList) {
      index += pos;
      newSql.deleteCharAt(index);
      String str = convertJavaTypeToSqlString(parameters.get(paramIndex));
      newSql.insert(index, str);
      pos += str.length() - 1;
      paramIndex++;
    }

    return newSql.toString();
  }

  /**
   * 将输入的java类型转成odps认识的sql类型，见https://help.aliyun.com/document_detail/159541.html
   * 需要额外指出的是：
   * java.sql.Date -> odps Date
   * java.sql.Time -> odps DateTime
   * java.sql.Timestamp -> odps Timestamp
   * java.util.Date -> odps Date
   * java.time.LocalDate -> odps Date
   * java.time.ZonedDateTime -> odps DateTime
   * java.time.Instant -> odps Timestamp
   *
   * @param x
   * @return
   * @throws SQLException
   */
  private String convertJavaTypeToSqlString(Object x) throws SQLException {
    if (Byte.class.isInstance(x)) {
      return String.format("%sY", x.toString());
    } else if (Short.class.isInstance(x)) {
      return String.format("%sS", x.toString());
    } else if (Integer.class.isInstance(x)) {
      return x.toString();
    } else if (Long.class.isInstance(x)) {
      return String.format("%sL", x.toString());
    } else if (Float.class.isInstance(x)) {
      return String.format("%sf", x.toString());
    } else if (Double.class.isInstance(x)) {
      return String.format("%s", x.toString());
    } else if (BigDecimal.class.isInstance(x)) {
      return String.format("%sBD", x.toString());
    } else if (String.class.isInstance(x)) {
      if (isIllegal((String) x)) {
        throw new IllegalArgumentException("");
      }
      return "'" + x + "'";
    } else if (byte[].class.isInstance(x)) {
      try {
        String charset = getConnection().getCharset();
        if (charset != null) {
          String str = new String((byte[]) x, charset);
          if (isIllegal(str)) {
            throw new IllegalArgumentException("");
          }
          return "'" + str + "'";
        } else {
          throw new SQLException("charset is null");
        }
      } catch (UnsupportedEncodingException e) {
        throw new SQLException(e);
      }
    } else if (java.util.Date.class.isInstance(x)) {
      Calendar.Builder calendarBuilder = new Calendar.Builder()
          .setCalendarType("iso8601")
          .setLenient(true);
      Calendar calendar = calendarBuilder.build();

      if (java.sql.Timestamp.class.isInstance(x)) {
        return String.format("TIMESTAMP'%s'", x);
      } else if (java.sql.Date.class.isInstance(x)) {
        // MaxCompute DATE
        DATE_FORMAT.get().setCalendar(calendar);
        return String.format("DATE'%s'", DATE_FORMAT.get().format(x));
      } else if (java.sql.Time.class.isInstance(x)) {
        DATETIME_FORMAT.get().setCalendar(calendar);
        return String.format("DATETIME'%s'", DATETIME_FORMAT.get().format(x));
      } else {
        // MaxCompute DATETIME
        DATE_FORMAT.get().setCalendar(calendar);
        return String.format("DATE'%s'", DATE_FORMAT.get().format(x));
      }
    } else if (x instanceof LocalDate) {
      return String.format("DATE'%s'", LOCAL_DATE_FORMAT.get().format((LocalDate) x));
    } else if (x instanceof ZonedDateTime) {
      return String.format("DATETIME'%s'",
                           ZONED_DATETIME_FORMAT.get().format((ZonedDateTime) x));
    } else if (x instanceof Instant) {
      return String.format("TIMESTAMP'%s'",
                           ZONED_TIMESTAMP_FORMAT.get().format((Instant) x));
    } else if (Boolean.class.isInstance(x)) {
      return x.toString().toUpperCase();
    } else if (x == null || x.equals(Types.NULL)) {
      return "NULL";
    } else if (Binary.class.isInstance(x)) {
      return String.format("unhex('%s')", x);
    } else if (Varchar.class.isInstance(x)) {
      if (isIllegal(x.toString())) {
        throw new IllegalArgumentException("");
      }
      return "'" + x.toString() + "'";
    } else if (Char.class.isInstance(x)) {
      if (isIllegal(x.toString())) {
        throw new IllegalArgumentException("");
      }
      return "'" + x.toString() + "'";
    } else {
      throw new SQLException("unrecognized Java class: " + x.getClass().getName());
    }
  }

  private boolean isIllegal(String str) {
    Matcher matcher = SQL_PATTERN.matcher(str);
    return matcher.find();
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
    setObject(parameterIndex, x);
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
      throws SQLException {
    setObject(parameterIndex, x);
  }

  @Override
  public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
    setObject(parameterIndex, x);
  }

  @Override
  public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength)
      throws SQLException {
    setObject(parameterIndex, x);
  }
}