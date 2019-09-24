package com.aliyun.odps.jdbc.utils.transformer.to.odps;

import java.sql.SQLException;

public class ToOdpsIntTransformer extends AbstractToOdpsTransformer {

  @Override
  public Object transform(Object o, String charset) throws SQLException {
    if (o == null) {
      return null;
    }

    if (Integer.class.isInstance(o)) {
      return o;
    } else {
      String errorMsg = getInvalidTransformationErrorMsg(o.getClass(), Integer.class);
      throw new SQLException(errorMsg);
    }
  }
}
