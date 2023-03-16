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
import java.io.InputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Binary;
import com.aliyun.odps.data.Char;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.Varchar;
import com.aliyun.odps.jdbc.utils.JdbcColumn;
import com.aliyun.odps.jdbc.utils.transformer.to.odps.AbstractToOdpsTransformer;
import com.aliyun.odps.jdbc.utils.transformer.to.odps.ToOdpsTransformerFactory;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.TunnelRecordWriter;
import com.aliyun.odps.utils.StringUtils;

public class OdpsPreparedStatement extends OdpsStatement implements PreparedStatement {

  private final String TABLE_NAME = "((\\w+\\.)?\\w+)";      // "proj.name" or "name"
  private final String PREP_VALUES = "\\((\\s*\\?\\s*)(,\\s*\\?\\s*)*\\)"; // "(?)" or "(?,?,...)"
  private final String
      SPEC_COLS =
      "\\(((\\s*\\w+\\s*)=((\\s*\\w+\\s*)|(\\s*'\\s*\\w+\\s*'\\s*)))(,(\\s*\\w+\\s*)=((\\s*\\w+\\s*)|(\\s*'\\s*\\w+\\s*'\\s*)))*\\)";
  // (p1=a) or (p1=a,p2=b,...) or (p1='a') ...


  private final String PREP_INSERT_WITH_SPEC_COLS =
      "(?i)^" + "\\s*" + "insert" + "\\s+" + "into" + "\\s+" + TABLE_NAME + "\\s+" + "partition" +
      SPEC_COLS + "\\s+" + "values" + "\\s*" + PREP_VALUES + "\\s*" + ";?\\s*$";

  private final String PREP_INSERT_WITH_SPEC_COLS_EXAMPLE =
      "INSERT INTO table (key1=value1, key2=value2) VALUES (?, ?);";

  private final String PREP_INSERT_WITHOUT_SPEC_COLS =
      "(?i)^" + "\\s*" + "insert" + "\\s+" + "into" + "\\s+" + TABLE_NAME + "\\s+" +
      "values" + "\\s*" + PREP_VALUES + "\\s*" + ";?\\s*$";

  private final String PREP_INSERT_WITHOUT_SPEC_COLS_EXAMPLE =
      "INSERT INTO table VALUES (?, ?, ?);";

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
      ZONED_DATETIME_FORMAT =
      ThreadLocal.withInitial(() -> DateTimeFormatter.ofPattern(JdbcColumn.ODPS_DATETIME_FORMAT)
          .withZone(ZoneId.systemDefault()));

  /**
   * The prepared sql template (immutable). e.g. insert into table FOO select * from BAR where id =
   * ? and weight = ?
   */
  private final String sql;

  private boolean verified = false;
  private String tableBatchInsertTo;

  private String partitionSpec;

  private int parametersNum;

  TableTunnel.UploadSession session;
  Record reuseRecord;
  int blocks;

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
    if (!verified) {
      boolean withSpecCols = sql.matches(PREP_INSERT_WITH_SPEC_COLS);
      boolean withoutSpecCols = sql.matches(PREP_INSERT_WITHOUT_SPEC_COLS);

      if (!withoutSpecCols && !withSpecCols) {
        throw new SQLException("batched statement only support following syntax: "
                               + PREP_INSERT_WITHOUT_SPEC_COLS_EXAMPLE + " or "
                               + PREP_INSERT_WITH_SPEC_COLS_EXAMPLE);
      }

      if (withoutSpecCols) {
        setSession(Pattern.compile(PREP_INSERT_WITHOUT_SPEC_COLS).matcher(sql), false);
      }

      if (withSpecCols) {
        setSession(Pattern.compile(PREP_INSERT_WITH_SPEC_COLS).matcher(sql), true);
      }
    }

    int batchedSize = batchedRows.size();
    // if no sql is batched, just return
    if (batchedSize == 0) {
      return new int[0];
    }

    getConnection().log
        .info(batchedSize + " records are going to be uploaded to table " + tableBatchInsertTo
              + " in batch");

    int[] updateCounts = new int[batchedSize];
    Arrays.fill(updateCounts, -1);

    try {
      long startTime = System.currentTimeMillis();
      TunnelRecordWriter recordWriter = (TunnelRecordWriter) session.openRecordWriter(blocks, true);
      List<Column> columns = session.getSchema().getColumns();
      for (int i = 0; i < batchedSize; i++) {
        Object[] row = batchedRows.get(i);
        for (int j = 0; j < columns.size(); j++) {
          AbstractToOdpsTransformer transformer =
              ToOdpsTransformerFactory.getTransformer(columns.get(j).getTypeInfo().getOdpsType());
          reuseRecord.set(j, transformer.transform(row[j], getConnection().getCharset()));
        }
        recordWriter.write(reuseRecord);
        updateCounts[i] = 1;
      }

      long duration = System.currentTimeMillis() - startTime;
      float megaBytesPerSec = (float) recordWriter.getTotalBytes() / 1024 / 1024 / duration * 1000;
      recordWriter.close();
      getConnection().log.info(
          String.format("It took me %d ms to insert %d records [%d], %.2f MiB/s", duration,
                        batchedSize,
                        blocks, megaBytesPerSec));
      blocks += 1;
    } catch (TunnelException e) {
      throw new SQLException(e);
    } catch (IOException e) {
      throw new SQLException(e);
    }

    clearBatch();
    return updateCounts;
  }

  private void setSession(Matcher matcher, boolean hasPartition) throws SQLException {
    if (matcher.find()) {
      tableBatchInsertTo = matcher.group(1);
      if (hasPartition) {
        partitionSpec = matcher.group(3);
      }
    } else {
      throw new SQLException("cannot extract table name or partition name in SQL: " + sql);
    }

    TableTunnel tunnel = new TableTunnel(getConnection().getOdps());
    // TODO 三层模型
    try {
      if (tableBatchInsertTo.contains(".")) {
        String[] splited = tableBatchInsertTo.split("\\.");
        String projectName = splited[0];
        String tableName = splited[1];

        if (hasPartition && !StringUtils.isNullOrEmpty(partitionSpec)) {
          Table table = getConnection().getOdps().tables().get(projectName, tableName);
          PartitionSpec partition = new PartitionSpec(partitionSpec);
          if (!table.hasPartition(partition)) {
            table.createPartition(partition);
          }
          session = tunnel.createUploadSession(projectName, tableName, partition);
        } else {
          session = tunnel.createUploadSession(projectName, tableName);
        }
      } else {
        String defaultProject = getConnection().getOdps().getDefaultProject();
        if (hasPartition && !StringUtils.isNullOrEmpty(partitionSpec)) {
          Table table = getConnection().getOdps().tables().get(defaultProject, tableBatchInsertTo);
          PartitionSpec partition = new PartitionSpec(partitionSpec);
          if (!table.hasPartition(partition)) {
            table.createPartition(partition);
          }
          session = tunnel.createUploadSession(defaultProject, tableBatchInsertTo, partition);
        } else {
          session = tunnel.createUploadSession(defaultProject, tableBatchInsertTo);
        }
      }
    } catch (TunnelException e) {
      throw new SQLException(e);
    } catch (OdpsException e) {
      throw new RuntimeException(e);
    }
    getConnection().log.info("create upload session id=" + session.getId());
    TableSchema schema = session.getSchema();
    reuseRecord = session.newRecord();
    int colNum = schema.getColumns().size();
    int valNum = batchedRows.get(0).length;
    if (valNum != colNum) {
      throw new SQLException(
          "the table has " + colNum + " columns, but insert " + valNum + " values");
    }

    blocks = 0;
    verified = true;
  }

  // Commit on close
  @Override
  public void close() throws SQLException {
    if (isClosed()) {
      return;
    }
    if (session != null && blocks > 0) {
      Long[] blockList = new Long[blocks];
      getConnection().log.info("commit session: " + blocks + " blocks");
      for (int i = 0; i < blocks; i++) {
        blockList[i] = Long.valueOf(i);
      }
      try {
        session.commit(blockList);
      } catch (TunnelException e) {
        throw new SQLException(e);
      } catch (IOException e) {
        throw new SQLException(e);
      }
    }
    super.close();
  }

  @Override
  public boolean execute() throws SQLException {
    return super.execute(updateSql(sql, parameters));
  }

  @Override
  public ResultSet executeQuery() throws SQLException {
    return super.executeQuery(updateSql(sql, parameters));
  }

  @Override
  public int executeUpdate() throws SQLException {
    return super.executeUpdate(updateSql(sql, parameters));
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    if (getExecuteInstance() == null) {
      this.execute();
    }
    return getResultSet().getMetaData();
  }

  @Override
  public ParameterMetaData getParameterMetaData() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setArray(int parameterIndex, Array x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, int length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, int length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setBlob(int parameterIndex, Blob x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    parameters.put(parameterIndex, x);
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, int length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setClob(int parameterIndex, Clob x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setClob(int parameterIndex, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setClob(int parameterIndex, Reader reader, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setNClob(int parameterIndex, NClob value) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setNString(int parameterIndex, String value) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setNull(int parameterIndex, int sqlType, String typeName)
      throws SQLException {
    // ODPS doesn't care the type of NULL. So the second parameter is simply ignored.
    parameters.put(parameterIndex, null);
  }

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
    } else if (x instanceof ZonedDateTime) {
      parameters.put(parameterIndex, x);
    } else if (x instanceof Instant) {
      parameters.put(parameterIndex, x);
    } else if (x instanceof Varchar) {
      parameters.put(parameterIndex, x);
    } else if (x instanceof Char) {
      parameters.put(parameterIndex, x);
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

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setRef(int parameterIndex, Ref x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setRowId(int parameterIndex, RowId x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setURL(int parameterIndex, URL x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setUnicodeStream(int parameterIndex, InputStream x, int length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  /**
   * Returns a new sql replacing the '?'s in the prepared sql to parameters.
   */
  private String updateSql(String sql, HashMap<Integer, Object> parameters) throws SQLException {
    if (!sql.contains("?")) {
      return sql;
    }

    StringBuilder newSql = new StringBuilder(sql);

    int paramIndex = 1;
    int pos = 0;

    while (pos < newSql.length()) {
      if (newSql.charAt(pos) == '?') {
        if (parameters.containsKey(paramIndex)) {
          newSql.deleteCharAt(pos);
          String str = convertJavaTypeToSqlString(parameters.get(paramIndex));
          newSql.insert(pos, str);
          pos += str.length() - 1;
        }
        paramIndex++;
      } else {
        pos++;
      }
    }

    return newSql.toString();
  }

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
    }else if (BigDecimal.class.isInstance(x)) {
      return String.format("%sBD", x.toString());
    } else if (Varchar.class.isInstance(x)) {
      return x.toString();
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
    } else if (x instanceof ZonedDateTime) {
      return String.format("DATETIME'%s'",
                           ZONED_DATETIME_FORMAT.get().format(((ZonedDateTime) x).toLocalDateTime()));
    } else if (x instanceof Instant) {
      ZonedDateTime
          zonedDateTime =
          ZonedDateTime.ofInstant((Instant) x, ZONED_DATETIME_FORMAT.get().getZone());
      return String.format("TIMESTAMP'%s'",
                           java.sql.Timestamp.valueOf(zonedDateTime.toLocalDateTime()).toString());
    } else if (Boolean.class.isInstance(x)) {
      return x.toString().toUpperCase();
    } else if (x == null || x.equals(Types.NULL)) {
      return "NULL";
    } else if (Binary.class.isInstance(x)) {
      return String.format("unhex('%s')", x);
    } else if (Varchar.class.isInstance(x)) {
      return x.toString();
    } else if (Char.class.isInstance(x)) {
      return x.toString();
    } else {
      throw new SQLException("unrecognized Java class: " + x.getClass().getName());
    }
  }

  private boolean isIllegal(String str) {
    Matcher matcher = SQL_PATTERN.matcher(str);
    return matcher.find();
  }

}