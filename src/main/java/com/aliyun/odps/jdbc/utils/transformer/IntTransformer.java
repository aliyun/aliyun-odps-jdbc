package com.aliyun.odps.jdbc.utils.transformer;

import java.sql.SQLException;

public class IntTransformer extends AbstractTransformer {

  @Override
  public Object transform(Object o, String charset) throws SQLException {
    if (o == null) {
      return 0;
    }

    if (Number.class.isInstance(o)) {
      return ((Number) o).intValue();
    } else if (o instanceof byte[]) {
      try {
        return Integer.parseInt(encodeBytes((byte[]) o, charset));
      } catch (NumberFormatException e) {
        String errorMsg = getTransformationErrMsg(encodeBytes((byte[]) o, charset), int.class);
        throw new SQLException(errorMsg);
      }
    } else {
      String errorMsg = getInvalidTransformationErrorMsg(o.getClass(), int.class);
      throw new SQLException(errorMsg);
    }
  }
}
