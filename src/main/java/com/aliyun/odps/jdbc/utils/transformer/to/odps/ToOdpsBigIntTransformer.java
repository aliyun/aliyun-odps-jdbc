package com.aliyun.odps.jdbc.utils.transformer.to.odps;

import java.sql.SQLException;

public class ToOdpsBigIntTransformer extends AbstractToOdpsTransformer {

  @Override
  public Object transform(Object o, String charset) throws SQLException {
    if (o == null) {
      return null;
    }

    if (Long.class.isInstance(o)) {
      return o;
    } else {
      String errorMsg = getInvalidTransformationErrorMsg(o.getClass(), Long.class);
      throw new SQLException(errorMsg);
    }
  }
}
