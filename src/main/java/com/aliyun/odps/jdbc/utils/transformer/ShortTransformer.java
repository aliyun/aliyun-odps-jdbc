package com.aliyun.odps.jdbc.utils.transformer;

import java.sql.SQLException;

public class ShortTransformer extends AbstractTransformer {

  @Override
  public Object transform(Object o, String charset) throws SQLException {
    if (o == null) {
      return 0;
    }

    if (Number.class.isInstance(o)) {
      return ((Number) o).shortValue();
    } else if (o instanceof byte[]) {
      try {
        return Short.parseShort(encodeBytes((byte[]) o, charset));
      } catch (NumberFormatException e) {
        String errorMsg = getTransformationErrMsg(encodeBytes((byte[]) o, charset), short.class);
        throw new SQLException(errorMsg);
      }
    } else {
      String errorMsg = getInvalidTransformationErrorMsg(o.getClass(), short.class);
      throw new SQLException(errorMsg);
    }
  }
}
