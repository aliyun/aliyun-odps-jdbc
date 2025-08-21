package com.aliyun.odps.jdbc.data;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.jdbc.OdpsResultSet;
import com.aliyun.odps.jdbc.utils.JdbcColumn;
import com.aliyun.odps.jdbc.utils.transformer.to.jdbc.ToJdbcTransformerFactory;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.TypeInfo;

/**
 * OdpsArray implements the java.sql.Array interface for MaxCompute array types.
 *
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class OdpsArray implements Array {

    private Object[] arrayData;
    private ArrayTypeInfo arrayTypeInfo;

    public OdpsArray(List<Object> arrayData, ArrayTypeInfo arrayTypeInfo) {
        if (arrayData == null || arrayTypeInfo == null) {
            throw new IllegalArgumentException("both arrayData and arrayTypeInfo cannot be null.");
        }
        this.arrayData = arrayData.toArray(new Object[0]);
        this.arrayTypeInfo = arrayTypeInfo;
    }

    public OdpsArray(Object[] arrayData, ArrayTypeInfo arrayTypeInfo) {
        if (arrayData == null || arrayTypeInfo == null) {
            throw new IllegalArgumentException("both arrayData and arrayTypeInfo cannot be null.");
        }
        this.arrayData = arrayData;
        this.arrayTypeInfo = arrayTypeInfo;
    }

    @Override
    public String getBaseTypeName() throws SQLException {
        return arrayTypeInfo.getElementTypeInfo().getTypeName();
    }

    @Override
    public int getBaseType() throws SQLException {
        return JdbcColumn.odpsTypeToSqlType(arrayTypeInfo.getElementTypeInfo().getOdpsType());
    }

    @Override
    public Object getArray() throws SQLException {
        return arrayData;
    }

    @Override
    public Object getArray(Map<String, Class<?>> map) throws SQLException {
        if (map == null || !map.containsKey(getBaseTypeName())) {
            return getArray();
        }
        Class<?> elementClass = map.get(getBaseTypeName());
        if (arrayData.length == 0) {
            return arrayData;
        }
        if (arrayData[0].getClass() == elementClass) {
            return arrayData;
        }
        throw new SQLFeatureNotSupportedException(
            "Not support convert " + arrayData[0].getClass() + " to " + elementClass.getName()
            + " yet.");
    }

    @Override
    public Object getArray(long index, int count) throws SQLException {
        if (index < 1 || index > arrayData.length || count < 0) {
            throw new SQLException("Invalid index or count");
        }

        int startIndex = (int) index - 1;
        int endIndex = Math.min(startIndex + count, arrayData.length);
        int length = endIndex - startIndex;

        Object[] result = new Object[length];
        System.arraycopy(arrayData, startIndex, result, 0, length);
        return result;
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
        if (map == null || !map.containsKey(getBaseTypeName())) {
            return getArray(index, count);
        }
        Class<?> elementClass = map.get(getBaseTypeName());
        if (arrayData.length == 0) {
            return arrayData;
        }
        if (arrayData[0].getClass() == elementClass) {
            return arrayData;
        }
        throw new SQLFeatureNotSupportedException(
            "Not support convert " + arrayData[0].getClass() + " to " + elementClass.getName()
            + " yet.");
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        throw new SQLFeatureNotSupportedException("getResultSet() is not supported");
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("getResultSet(Map) is not supported");
    }

    @Override
    public ResultSet getResultSet(long index, int count) throws SQLException {
        throw new SQLFeatureNotSupportedException("getResultSet(long, int) is not supported");
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map)
        throws SQLException {
        throw new SQLFeatureNotSupportedException("getResultSet(long, int, Map) is not supported");
    }

    @Override
    public void free() throws SQLException {
        arrayData = null;
        arrayTypeInfo = null;
    }
}
