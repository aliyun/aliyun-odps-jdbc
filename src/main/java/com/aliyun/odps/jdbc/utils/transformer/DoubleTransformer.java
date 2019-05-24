package com.aliyun.odps.jdbc.utils.transformer;

import java.sql.SQLException;

public class DoubleTransformer extends AbstractTransformer {

  @Override
  public Object transform(Object o, String charset) throws SQLException {
    if (o == null) {
      return (double) 0;
    }

    if (Number.class.isInstance(o)) {
      return ((Number) o).doubleValue();
    } else if (o instanceof byte[]) {
      try {
        return Double.parseDouble(encodeBytes((byte[]) o, charset));
      } catch (NumberFormatException e) {
        String errorMsg = getTransformationErrMsg(encodeBytes((byte[]) o, charset), double.class);
        throw new SQLException(errorMsg);
      }
    } else {
      String errorMsg = getInvalidTransformationErrorMsg(o.getClass(), double.class);
      throw new SQLException(errorMsg);
    }
  }
}
