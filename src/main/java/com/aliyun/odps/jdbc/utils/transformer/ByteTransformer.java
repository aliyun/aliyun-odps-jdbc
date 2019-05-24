package com.aliyun.odps.jdbc.utils.transformer;

import java.sql.SQLException;

public class ByteTransformer extends AbstractTransformer {

  @Override
  public Object transform(Object o, String charset) throws SQLException {
    if (o == null) {
      return 0;
    }

    if (Number.class.isInstance(o)) {
      return ((Number) o).byteValue();
    } else {
      String errorMsg = getInvalidTransformationErrorMsg(o.getClass(), byte.class);
      throw new SQLException(errorMsg);
    }
  }
}
