package com.aliyun.odps.jdbc.utils.transformer.to.odps;

import java.sql.SQLException;

public class ToOdpsFloatTransformer extends AbstractToOdpsTransformer {

  @Override
  public Object transform(Object o, String charset) throws SQLException {
    if (o == null) {
      return null;
    }

    if (Float.class.isInstance(o)) {
      return o;
    } else {
      String errorMsg = getInvalidTransformationErrorMsg(o.getClass(), Float.class);
      throw new SQLException(errorMsg);
    }
  }
}
