package com.aliyun.odps.jdbc.impl;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OdpsResultSetMetaData extends WrapperAdapter implements ResultSetMetaData {
    private final List<String> columnNames;
    private final List<Integer> columnTypes;
    private Map<String, Integer> columnMap;

    OdpsResultSetMetaData(List<String> columnNames, List<Integer> columnTypes) {
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
        return columnNames.size();
    }

    @Override public int getColumnDisplaySize(int column) throws SQLException {
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
            for (int i = 0; i < columnNames.size(); ++i) {
                columnMap.put(columnNames.get(i), i + 1);
                columnMap.put(columnNames.get(i).toLowerCase(), i + 1);
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

    @Override public String getColumnLabel(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public String getColumnName(int column) throws SQLException {
        return columnNames.get(toZeroIndex(column));
    }

    @Override public int getColumnType(int column) throws SQLException {
        return columnTypes.get(toZeroIndex(column));
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

    protected int toZeroIndex(int column) {
        if (column <= 0 || column > columnNames.size()) {
            throw new IllegalArgumentException();
        }
        return column - 1;
    }
}
