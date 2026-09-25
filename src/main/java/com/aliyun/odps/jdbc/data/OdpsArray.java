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
        return applyTypeMap(arrayData, map);
    }

    @Override
    public Object getArray(long index, int count) throws SQLException {
        if (arrayData == null) {
            // Released by free(). The zero-argument getArray() returns null for the same state --
            // and that is what 3.10.14 pinned -- so a sub-range must not fail with an
            // NullPointerException here either.
            return null;
        }
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
        // The window asked for by index/count, then the type map -- never the whole array, which
        // is what this method used to hand back whenever the map happened to match.
        return applyTypeMap((Object[]) getArray(index, count), map);
    }

    /**
     * Check a {@code Map} type mapping against the elements actually being returned. NULL
     * elements carry no class to compare, so the first non-null element decides; an array that is
     * empty, all-NULL or already released has nothing to contradict the mapping.
     */
    private Object[] applyTypeMap(Object[] data, Map<String, Class<?>> map) throws SQLException {
        if (map == null || map.isEmpty() || !map.containsKey(getBaseTypeName())) {
            return data;
        }
        Class<?> elementClass = map.get(getBaseTypeName());
        Object probe = null;
        if (data != null) {
            for (Object element : data) {
                if (element != null) {
                    probe = element;
                    break;
                }
            }
        }
        if (probe == null || probe.getClass() == elementClass) {
            return data;
        }
        throw new SQLFeatureNotSupportedException(
            "Not support convert " + probe.getClass() + " to " + elementClass.getName()
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
        // The element type is metadata of the column, not of the values, and callers such as BI
        // tools read it while building a result set. Dropping it here made getBaseTypeName() and
        // getBaseType() fail with a NullPointerException after free(); the released-state contract
        // of this class is "values are gone (null), type still known".
    }
}
