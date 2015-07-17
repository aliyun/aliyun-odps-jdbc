package com.aliyun.odps.jdbc.impl;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;

public class OdpsQueryResultSet extends OdpsResultSet implements ResultSet {

    private OdpsResultSetMetaData meta;

    private RecordReader recordReader;
    private Record record;
    private Object[] columnValues;


    OdpsQueryResultSet(OdpsStatement stmt, OdpsResultSetMetaData meta, RecordReader reader)
        throws SQLException {
        super(stmt);
        this.meta = meta;
        this.recordReader = reader;
    }

    @Override public boolean next() throws SQLException {
        try {
            record = recordReader.read();
            if (record == null) {
                return false;
            }
            columnValues = record.toArray();
            return true;
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override public OdpsResultSetMetaData getMetaData() throws SQLException {
        return meta;
    }

    @Override public Object getObject(int columnIndex) throws SQLException {
        return columnValues != null ? columnValues[toZeroIndex(columnIndex)] : null;
    }

    @Override public Object getObject(String columnName) throws SQLException {
        int columnIndex = meta.getColumnIndex(columnName);
        return getObject(columnIndex);
    }

    protected int toZeroIndex(int column) {
        if (column <= 0 || column > columnValues.length) {
            throw new IllegalArgumentException();
        }
        return column - 1;
    }

    /**
     * Convert a string literal to a Java object with a specified type
     * @param literal the sting literal, e.g. "6.214"
     * @param type OdpsType
     * @return a java object
     */
    protected Object evaluate(String literal, OdpsType type) {
        if (literal == null) {
            return null;
        }
        if (type == OdpsType.BIGINT) {
            return Long.valueOf(literal);
        } else if (type == OdpsType.BOOLEAN) {
            return Boolean.valueOf(literal);
        } else if (type == OdpsType.DOUBLE) {
            return Double.valueOf(literal);
        } else if (type == OdpsType.STRING) {
            return literal;
        } else if (type == OdpsType.DATETIME) {
            return Timestamp.valueOf(literal);
        } else {
            return null;
        }
    }
}
