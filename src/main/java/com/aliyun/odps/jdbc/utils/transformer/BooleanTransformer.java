package com.aliyun.odps.jdbc.utils.transformer;

import java.sql.SQLException;

public class BooleanTransformer extends AbstractTransformer {

  @Override
  public Object transform(Object o, String charset) throws SQLException {
    if (o == null) {
      return false;
    }

    if (Boolean.class.isInstance(o)) {
      return o;
    } else if (Number.class.isInstance(o)) {
      return ((Number) o).intValue() != 0;
    } else if (o instanceof byte[]) {
      return !"0".equals(encodeBytes((byte[]) o, charset));
    } else {
      String errorMsg = getInvalidTransformationErrorMsg(o.getClass(), boolean.class);
      throw new SQLException(errorMsg);
    }
  }
}
