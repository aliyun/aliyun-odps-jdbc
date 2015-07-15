package com.aliyun.odps.jdbc.impl;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.HashMap;
import java.util.Map;

public class OdpsResultSetMetaData extends WrapperAdapter implements ResultSetMetaData {
    private final String[] columnNames;
    private final int[] columnTypes;

    private Map<String, Integer> columnMap;

    OdpsResultSetMetaData(String[] columnNames, int[] columnTypes) {
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
    }

    @Override public String getCatalogName(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public String getColumnClassName(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public int getColumnCount() throws SQLException {
        return columnNames.length;
    }

    @Override public int getColumnDisplaySize(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public String getColumnLabel(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public String getColumnName(int column) throws SQLException {
        return columnNames[toZeroIndex(column)];
    }

    @Override public int getColumnType(int column) throws SQLException {
        return columnTypes[toZeroIndex(column)];
    }

    @Override public String getColumnTypeName(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public int getPrecision(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public int getScale(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public String getSchemaName(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public String getTableName(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public boolean isAutoIncrement(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public boolean isCaseSensitive(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public boolean isCurrency(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public boolean isDefinitelyWritable(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public int isNullable(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public boolean isReadOnly(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public boolean isSearchable(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public boolean isSigned(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public boolean isWritable(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    /**
     * Used by other classes for querying the index from column name
     *
     * @param name column name
     * @return column index
     */
    public int getColumnIndex(String name) {
        if (columnMap == null) {
            columnMap = new HashMap<String, Integer>();
            for (int i = 0; i < columnNames.length; ++i) {
                columnMap.put(columnNames[i], i + 1);
                columnMap.put(columnNames[i].toLowerCase(), i + 1);
            }
        }

        Integer index = columnMap.get(name);
        if (index == null) {
            String lowerName = name.toLowerCase();
            if (lowerName == name) {
                return -1;
            }

            index = columnMap.get(name);
        }

        if (index == null) {
            return -1;
        }

        return index.intValue();
    }

    protected int toZeroIndex(int column) {
        if (column <= 0 || column > columnNames.length) {
            throw new IllegalArgumentException();
        }
        return column - 1;
    }
}
