package com.aliyun.odps.jdbc.utils.transformer.to.odps;

import java.sql.SQLException;
import java.util.List;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class ToOdpsArrayTransformer extends AbstractToOdpsTransformer {

    @Override
    public Object transform(Object o, String charset) throws SQLException {
        if (o == null) {
            return null;
        }

        if (List.class.isInstance(o)) {
            return o;
        } else {
            String errorMsg = getInvalidTransformationErrorMsg(o.getClass(), Long.class);
            throw new SQLException(errorMsg);
        }
    }
}