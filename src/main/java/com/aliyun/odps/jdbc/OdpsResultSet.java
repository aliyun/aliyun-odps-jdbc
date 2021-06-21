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


import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;
import java.util.TimeZone;

import com.aliyun.odps.jdbc.utils.transformer.to.jdbc.AbstractToJdbcDateTypeTransformer;
import com.aliyun.odps.jdbc.utils.transformer.to.jdbc.AbstractToJdbcTransformer;
import com.aliyun.odps.jdbc.utils.transformer.to.jdbc.ToJdbcTransformerFactory;


public abstract class OdpsResultSet extends WrapperAdapter implements ResultSet {

  private OdpsResultSetMetaData meta;
  private OdpsStatement stmt;
  protected OdpsConnection conn;
  private boolean wasNull = false;

  private SQLWarning warningChain = null;

  OdpsResultSet(OdpsConnection conn, OdpsStatement stmt, OdpsResultSetMetaData meta) {
    this.stmt = stmt;
    this.meta = meta;
    this.conn = conn;
  }

  @Override
  public int getFetchDirection() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getFetchSize() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    conn.log.warn("Unsupported method call: OdpsResultSet#setFetchSize, ignored");
  }

  @Override
  public boolean absolute(int row) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void afterLast() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void beforeFirst() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void cancelRowUpdates() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void clearWarnings() throws SQLException {
    warningChain = null;
  }

  @Override
  public void deleteRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean first() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Array getArray(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Array getArray(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public InputStream getAsciiStream(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    Object val = getInnerObject(columnIndex);
    return (BigDecimal) transformToJdbcType(val, BigDecimal.class);
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    int columnIndex = findColumn(columnLabel);
    return getBigDecimal(columnIndex);
  }

  /**
   * The column index can be retrieved by name through the ResultSetMetaData.
   *
   * @param columnLabel
   *     the name of the column
   * @return the column index
   * @throws SQLException invalid label
   */
  @Override
  public int findColumn(String columnLabel) throws SQLException {
    int index = getMetaData().getColumnIndex(columnLabel);
    if (index == -1) {
      throw new SQLException("the column label is invalid");
    }
    return index;
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  // Accessor
  abstract Object[] rowAtCursor() throws SQLException;

  // Do not call this method within OdpsResultSet class, call getInnerObject instead
  @Override
  public Object getObject(int columnIndex) throws SQLException {
    Object obj = getInnerObject(columnIndex);

    if (obj instanceof byte[]) {
      String charset = conn.getCharset();
      return AbstractToJdbcTransformer.encodeBytes((byte[]) obj, charset);
    }
    return obj;
  }

  // The implementation stores STRING as byte[], but JDBC can only see String.
  // We need this method to cover since we only want to convert bytes[] to string as
  // late as in the real getObject().
  private Object getInnerObject(int columnIndex) throws SQLException {
    Object[] row = rowAtCursor();

    if (columnIndex < 1 || columnIndex > row.length) {
      throw new SQLException("column index must be >=1 and <=" + rowAtCursor().length);
    }

    Object obj = row[columnIndex - 1];
    wasNull = (obj == null);
    return obj;
  }

  @Override
  public Object getObject(String columnLabel) throws SQLException {
    int columnIndex = findColumn(columnLabel);
    return getObject(columnIndex);
  }

  @Override
  public Object getObject(int columnIndex, Map<String, Class<?>> map)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Object getObject(String columnLabel, Map<String, Class<?>> map)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Blob getBlob(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Blob getBlob(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    Object obj = getInnerObject(columnIndex);
    return (Boolean) transformToJdbcType(obj, boolean.class);
  }

  @Override
  public boolean getBoolean(String columnLabel) throws SQLException {
    int columnIndex = findColumn(columnLabel);
    return getBoolean(columnIndex);
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    Object obj = getInnerObject(columnIndex);
    return (Byte) transformToJdbcType(obj, byte.class);
  }

  @Override
  public byte getByte(String columnLabel) throws SQLException {
    int columnIndex = findColumn(columnLabel);
    return getByte(columnIndex);
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    Object obj = getInnerObject(columnIndex);
    return (byte[]) transformToJdbcType(obj, byte[].class);
  }

  @Override
  public byte[] getBytes(String columnLabel) throws SQLException {
    int columnIndex = findColumn(columnLabel);
    return getBytes(columnIndex);
  }

  @Override
  public Reader getCharacterStream(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Reader getCharacterStream(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Clob getClob(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Clob getClob(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getConcurrency() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getCursorName() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public java.sql.Date getDate(int columnIndex) throws SQLException {
    return getDate(columnIndex, null);
  }

  @Override
  public java.sql.Date getDate(String columnLabel) throws SQLException {
    int columnIndex = findColumn(columnLabel);
    return getDate(columnIndex);
  }

  @Override
  public java.sql.Date getDate(int columnIndex, Calendar cal) throws SQLException {
    Object obj = getInnerObject(columnIndex);
    return (java.sql.Date) transformToJdbcType(obj, java.sql.Date.class, cal);
  }

  @Override
  public java.sql.Date getDate(String columnLabel, Calendar cal) throws SQLException {
    int columnIndex = findColumn(columnLabel);
    return getDate(columnIndex, cal);
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    Object obj = getInnerObject(columnIndex);
    return (Double) transformToJdbcType(obj, double.class);
  }

  @Override
  public double getDouble(String columnLabel) throws SQLException {
    int columnIndex = findColumn(columnLabel);
    return getDouble(columnIndex);
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException {
    Object obj = getInnerObject(columnIndex);
    return (Float) transformToJdbcType(obj, float.class);
  }

  @Override
  public float getFloat(String columnLabel) throws SQLException {
    int columnIndex = findColumn(columnLabel);
    return getFloat(columnIndex);
  }

  @Override
  public int getHoldability() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    Object obj = getInnerObject(columnIndex);
    return (Integer) transformToJdbcType(obj, int.class);
  }

  @Override
  public int getInt(String columnLabel) throws SQLException {
    int columnIndex = findColumn(columnLabel);
    return getInt(columnIndex);
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    Object obj = getInnerObject(columnIndex);
    return (Long) transformToJdbcType(obj, long.class);
  }

  @Override
  public long getLong(String columnLabel) throws SQLException {
    int columnIndex = findColumn(columnLabel);
    return getLong(columnIndex);
  }

  @Override
  public OdpsResultSetMetaData getMetaData() throws SQLException {
    return meta;
  }

  @Override
  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Reader getNCharacterStream(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public NClob getNClob(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public NClob getNClob(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getNString(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    Object obj = getInnerObject(columnIndex);
    return (String) transformToJdbcType(obj, String.class, null);
  }

  @Override
  public String getNString(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getString(String columnLabel) throws SQLException {
    int columnIndex = findColumn(columnLabel);
    return getString(columnIndex);
  }

  @Override
  public Ref getRef(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Ref getRef(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public RowId getRowId(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public RowId getRowId(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    Object obj = getInnerObject(columnIndex);
    return (Short) transformToJdbcType(obj, short.class);
  }

  @Override
  public short getShort(String columnLabel) throws SQLException {
    int columnIndex = findColumn(columnLabel);
    return getShort(columnIndex);
  }

  @Override
  public OdpsStatement getStatement() throws SQLException {
    return stmt;
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException {
    return getTime(columnIndex, null);
  }

  @Override
  public Time getTime(String columnLabel) throws SQLException {
    int columnIndex = findColumn(columnLabel);
    return getTime(columnIndex);
  }

  @Override
  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    Object obj = getInnerObject(columnIndex);
    return (java.sql.Time) transformToJdbcType(obj, java.sql.Time.class, cal);
  }

  @Override
  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    int columnIndex = findColumn(columnLabel);
    return getTime(columnIndex, cal);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    return getTimestamp(columnIndex, null);
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    int columnIndex = findColumn(columnLabel);
    return getTimestamp(columnIndex);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
    Object obj = getInnerObject(columnIndex);
    return (Timestamp) transformToJdbcType(obj, Timestamp.class, cal);
  }

  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
    int columnIndex = findColumn(columnLabel);
    return getTimestamp(columnIndex, cal);
  }

  @Override
  public int getType() throws SQLException {
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public URL getURL(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public URL getURL(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public InputStream getUnicodeStream(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return warningChain;
  }

  @Override
  public void insertRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isClosed() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isFirst() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isLast() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean last() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void moveToCurrentRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void moveToInsertRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean previous() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void refreshRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean rowInserted() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  private Object transformToJdbcType(Object o, Class jdbcCls) throws SQLException {
    AbstractToJdbcTransformer transformer = ToJdbcTransformerFactory.getTransformer(jdbcCls);
    return transformer.transform(o, conn.getCharset());
  }

  private Object transformToJdbcType(Object o, Class jdbcCls, Calendar cal) throws SQLException {
    AbstractToJdbcTransformer transformer = ToJdbcTransformerFactory.getTransformer(jdbcCls);

    TimeZone timeZone = null;

    if (stmt != null) {
      String sessionTimeZoneId =
          stmt.getSqlTaskProperties().getProperty("odps.sql.timezone", null);
      if (sessionTimeZoneId != null) {
        timeZone = TimeZone.getTimeZone(sessionTimeZoneId);
      } else {
        timeZone = conn.isUseProjectTimeZone() ? conn.getProjectTimeZone() : null;
      }
    }

    return ((AbstractToJdbcDateTypeTransformer) transformer).transform(o, conn.getCharset(), timeZone);
  }

  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateArray(String columnLabel, Array x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(String columnLabel, Blob x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBoolean(String columnLabel, boolean x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateByte(String columnLabel, byte x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBytes(String columnLabel, byte[] x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, int length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(String columnLabel, Clob x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(int columnIndex, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(String columnLabel, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(int columnIndex, Reader reader, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(String columnLabel, Reader reader, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateDate(int columnIndex, java.sql.Date x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateDate(String columnLabel, java.sql.Date x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateDouble(String columnLabel, double x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateFloat(String columnLabel, float x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateInt(int columnIndex, int x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateInt(String columnLabel, int x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateLong(int columnIndex, long x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateLong(String columnLabel, long x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNString(int columnIndex, String nString) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNString(String columnLabel, String nString) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNull(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNull(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateObject(String columnLabel, Object x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateRef(String columnLabel, Ref x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateRowId(int columnIndex, RowId x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateRowId(String columnLabel, RowId x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateShort(int columnIndex, short x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateShort(String columnLabel, short x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateString(int columnIndex, String x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateString(String columnLabel, String x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateTime(String columnLabel, Time x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean wasNull() throws SQLException {
    return wasNull;
  }
}
