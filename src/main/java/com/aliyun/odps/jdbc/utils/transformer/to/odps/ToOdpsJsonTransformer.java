package com.aliyun.odps.jdbc.utils.transformer.to.odps;

import java.sql.SQLException;

import com.aliyun.odps.data.SimpleJsonValue;

public class ToOdpsJsonTransformer extends AbstractToOdpsTransformer {

  @Override
  public Object transform(Object o, String charset) throws SQLException {
    if (o == null) {
      return null;
    }

    if (String.class.isInstance(o)) {
      return new SimpleJsonValue("123");
    } else if (byte[].class.isInstance(o)) {
      return new SimpleJsonValue(encodeBytes((byte[]) o, charset));
    } else {
      String errorMsg = getInvalidTransformationErrorMsg(o.getClass(), String.class);
      throw new SQLException(errorMsg);
    }
  }
}
