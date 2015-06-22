package com.aliyun.odps.jdbc.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Iterator;

import com.aliyun.odps.Function;

public class OdpsFunctionsResultSet extends OdpsResultSet implements ResultSet {

    private Iterator<Function>    iterator;

    private OdpsResultSetMetaData meta;

    private Function              function;

    OdpsFunctionsResultSet(Iterator<Function> iterator) throws SQLException{
        super(null);
        this.iterator = iterator;
    }

    @Override
    public boolean next() throws SQLException {
        boolean hasNext = iterator.hasNext();

        if (hasNext) {
            function = iterator.next();
        } else {
            function = null;
        }

        return hasNext;
    }

    @Override
    public void close() throws SQLException {

    }

    @Override
    public OdpsResultSetMetaData getMetaData() throws SQLException {
        if (meta != null) {
            return meta;
        }

        String[] columns = new String[] { "FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "REMARKS",
                "FUNCTION_TYPE", "SPECIFIC_NAME" };
        int[] types = new int[columns.length];
        Arrays.fill(types, Types.VARCHAR);

        meta = new OdpsResultSetMetaData(columns, types);

        return meta;
    }

    @Override
    Object getObject0(int columnIndex) throws SQLException {
        switch (columnIndex) {
            case 1: // FUNCTION_CAT
                return null;
            case 2: // FUNCTION_SCHEM
                return null;
            case 3: // FUNCTION_NAME
                return function.getName();
            case 4: // REMARKS
                return null;
            case 5: // FUNCTION_TYPE
                return null;
            case 6: // SPECIFIC_NAME
                return null;
            default:
                return null;
        }
    }

}
