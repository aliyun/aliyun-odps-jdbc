package com.aliyun.odps.jdbc.utils.transformer;

import java.sql.SQLException;

public class LongTransformer extends AbstractTransformer {

  @Override
  public Object transform(Object o, String charset) throws SQLException {
    if (o == null) {
      return (long) 0;
    }

    if (Number.class.isInstance(o)) {
      return ((Number) o).longValue();
    } else if (o instanceof byte[]) {
      try {
        return Long.parseLong(encodeBytes((byte[]) o, charset));
      } catch (NumberFormatException e) {
        String errorMsg = getTransformationErrMsg(encodeBytes((byte[]) o, charset), long.class);
        throw new SQLException(errorMsg);
      } catch (SQLException e) {
        String errorMsg = getTransformationErrMsg(encodeBytes((byte[]) o, charset), long.class);
        throw new SQLException(errorMsg);
      }
    } else {
      String errorMsg = getInvalidTransformationErrorMsg(o.getClass(), long.class);
      throw new SQLException(errorMsg);
    }
  }
}
