package com.aliyun.odps.jdbc.utils.transformer.to.odps;

import java.sql.SQLException;

public class ToOdpsStringTransformer extends AbstractToOdpsTransformer {

  @Override
  public Object transform(Object o, String charset) throws SQLException {
    if (o == null) {
      return null;
    }

    if (String.class.isInstance(o)) {
      return o;
    } else if(byte[].class.isInstance(o)) {
      return encodeBytes((byte[]) o, charset);
    } else {
      String errorMsg = getInvalidTransformationErrorMsg(o.getClass(), String.class);
      throw new SQLException(errorMsg);
    }
  }
}
