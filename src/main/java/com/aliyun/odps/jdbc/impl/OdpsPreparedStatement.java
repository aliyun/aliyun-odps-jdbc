package com.aliyun.odps.jdbc.impl;

import java.io.InputStream;
import java.io.Reader;
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
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.SQLFeatureNotSupportedException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;

public class OdpsPreparedStatement extends OdpsStatement implements PreparedStatement {
    /**
     * The prepared sql template (immutable).
     */
    private final String sql;

    /**
     * The parameters for the prepared sql (index=>parameter).
     */
    private final HashMap<Integer, String> parameters = new HashMap<Integer, String>();

    OdpsPreparedStatement(OdpsConnection conn, String sql) {
        super(conn);
        this.sql = sql;
    }

    @Override public void addBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public void clearParameters() throws SQLException {
        parameters.clear();
    }

    @Override public boolean execute() throws SQLException {
        return super.execute(updateSql(sql, parameters));
    }

    @Override public ResultSet executeQuery() throws SQLException {
        return super.executeQuery(updateSql(sql, parameters));
    }

    @Override public int executeUpdate() throws SQLException {
        return super.executeUpdate(updateSql(sql, parameters));
    }

    @Override public ResultSetMetaData getMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public void setAsciiStream(int parameterIndex, InputStream x, int length)
        throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public void setAsciiStream(int parameterIndex, InputStream x, long length)
        throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        parameters.put(parameterIndex, "" + x);
    }

    @Override public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public void setBinaryStream(int parameterIndex, InputStream x, int length)
        throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public void setBinaryStream(int parameterIndex, InputStream x, long length)
        throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public void setBlob(int parameterIndex, InputStream inputStream, long length)
        throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        parameters.put(parameterIndex, "" + x);
    }

    @Override public void setByte(int parameterIndex, byte x) throws SQLException {
        parameters.put(parameterIndex, "" + x);
    }

    @Override public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public void setCharacterStream(int parameterIndex, Reader reader)
        throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public void setCharacterStream(int parameterIndex, Reader reader, int length)
        throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public void setCharacterStream(int parameterIndex, Reader reader, long length)
        throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public void setClob(int parameterIndex, Reader reader, long length)
        throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public void setDate(int parameterIndex, Date x) throws SQLException {
        SimpleDateFormat formatter = new SimpleDateFormat(TypeUtils.DEFFAULT_DATE_FORMAT);
        String dstr = formatter.format(x);
        parameters.put(parameterIndex, "cast('" + dstr + "' as datetime)");
    }

    @Override public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public void setDouble(int parameterIndex, double x) throws SQLException {
        parameters.put(parameterIndex, "" + x);
    }

    @Override public void setFloat(int parameterIndex, float x) throws SQLException {
        parameters.put(parameterIndex, "" + x);
    }

    @Override public void setInt(int parameterIndex, int x) throws SQLException {
        parameters.put(parameterIndex, "" + x);
    }

    @Override public void setLong(int parameterIndex, long x) throws SQLException {
        parameters.put(parameterIndex, "" + x);
    }

    @Override public void setNCharacterStream(int parameterIndex, Reader value)
        throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public void setNCharacterStream(int parameterIndex, Reader value, long length)
        throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public void setNClob(int parameterIndex, Reader reader, long length)
        throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public void setNString(int parameterIndex, String value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public void setNull(int parameterIndex, int sqlType) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public void setNull(int parameterIndex, int sqlType, String typeName)
        throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public void setObject(int parameterIndex, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public void setObject(int parameterIndex, Object x, int targetSqlType)
        throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
        throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public void setShort(int parameterIndex, short x) throws SQLException {
        parameters.put(parameterIndex, "" + x);
    }

    @Override public void setString(int parameterIndex, String x) throws SQLException {
        parameters.put(parameterIndex, "'"+x+"'");
    }

    @Override public void setTime(int parameterIndex, Time x) throws SQLException {
        SimpleDateFormat formatter = new SimpleDateFormat(TypeUtils.DEFFAULT_DATE_FORMAT);
        String dstr = formatter.format(x);
        parameters.put(parameterIndex, "cast('" + dstr + "' as datetime)");
    }

    @Override public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        SimpleDateFormat formatter = new SimpleDateFormat(TypeUtils.DEFFAULT_DATE_FORMAT);
        String dstr = formatter.format(x);
        parameters.put(parameterIndex, "cast('" + dstr + "' as datetime)");
    }

    @Override public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
        throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public void setURL(int parameterIndex, URL x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public void setUnicodeStream(int parameterIndex, InputStream x, int length)
        throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    /**
     * Returns a new sql replacing the '?'s in the prepared sql to parameters.
     */
    private String updateSql(String sql, HashMap<Integer, String> parameters) {
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
                    String str = parameters.get(paramIndex);
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
}
