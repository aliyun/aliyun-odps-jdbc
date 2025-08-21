package com.aliyun.odps.jdbc.utils.transformer.to.jdbc;

import java.sql.SQLException;
import java.util.List;

import com.aliyun.odps.jdbc.data.OdpsArray;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.TypeInfo;

/**
 * Transformer for converting MaxCompute array types to JDBC Array objects.
 *
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class ToJdbcArrayTransformer extends AbstractToJdbcTransformer {

    @Override
    public Object transform(Object o, String charset) throws SQLException {
        throw new SQLException("Should not reach this implement.");
    }

    public Object transform(Object o, String charset, TypeInfo odpsType) throws SQLException {
        if (o instanceof List) {
            return new OdpsArray((List) o, (ArrayTypeInfo) odpsType);
        }
        throw new SQLException(
            "Cannot transform ODPS-SDK Java class " + o.getClass().getName() + " to "
            + java.sql.Array.class.getName());
    }
}
