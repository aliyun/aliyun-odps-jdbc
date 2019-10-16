package com.aliyun.odps.jdbc.utils.transformer.to.odps;

import java.sql.SQLException;
import java.sql.Timestamp;

public class ToOdpsTimeStampTransformer extends AbstractToOdpsTransformer {

  @Override
  public Object transform(Object o, String charset) throws SQLException {
    if (o == null) {
      return null;
    }

    if (Timestamp.class.isInstance(o)) {
      return o;
    } else {
      String errorMsg = getInvalidTransformationErrorMsg(o.getClass(), Timestamp.class);
      throw new SQLException(errorMsg);
    }
  }
}
