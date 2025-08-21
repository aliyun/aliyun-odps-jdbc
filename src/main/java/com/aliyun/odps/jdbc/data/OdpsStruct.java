package com.aliyun.odps.jdbc.data;

import java.sql.SQLFeatureNotSupportedException;
import java.sql.Struct;
import java.sql.SQLException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;

/**
 * OdpsStruct implements the java.sql.Struct interface for MaxCompute struct types.
 *
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class OdpsStruct implements Struct {

    private Object[] attributes;
    private StructTypeInfo typeInfo;

    public OdpsStruct(Object[] attributes, StructTypeInfo typeInfo) {
        if (attributes == null || typeInfo == null) {
            throw new IllegalArgumentException("attributes, typeInfo cannot be null.");
        }
        this.attributes = attributes;
        this.typeInfo = typeInfo;
    }

    public StructTypeInfo getTypeInfo() {
        return typeInfo;
    }

    @Override
    public String getSQLTypeName() throws SQLException {
        return typeInfo.getTypeName();
    }

    @Override
    public Object[] getAttributes() throws SQLException {
        return attributes;
    }

    @Override
    public Object[] getAttributes(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            "getAttributes(Map<String, Class<?>> map) is not supported.");
    }
}