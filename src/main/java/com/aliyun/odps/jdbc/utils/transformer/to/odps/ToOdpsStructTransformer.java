package com.aliyun.odps.jdbc.utils.transformer.to.odps;

import java.sql.SQLException;
import java.util.List;

import com.aliyun.odps.data.Struct;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class ToOdpsStructTransformer extends AbstractToOdpsTransformer {

    @Override
    public Object transform(Object o, String charset) throws SQLException {
        if (o == null) {
            return null;
        }

        if (Struct.class.isInstance(o)) {
            return o;
        } else {
            String errorMsg = getInvalidTransformationErrorMsg(o.getClass(), Long.class);
            throw new SQLException(errorMsg);
        }
    }
}