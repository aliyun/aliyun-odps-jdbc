package com.aliyun.odps.jdbc.utils.transformer;

import java.math.BigDecimal;
import java.sql.SQLException;

public class BigDecimalTransformer extends AbstractTransformer {

  @Override
  public Object transform(Object o, String charset) throws SQLException {
    if (o == null) {
      return null;
    }

    if (BigDecimal.class.isInstance(o)) {
      return o;
    } else {
      String errorMsg = getInvalidTransformationErrorMsg(o.getClass(), BigDecimal.class);
      throw new SQLException(errorMsg);
    }
  }
}
