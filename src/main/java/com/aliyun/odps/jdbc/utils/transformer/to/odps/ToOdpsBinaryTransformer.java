package com.aliyun.odps.jdbc.utils.transformer.to.odps;

import java.sql.SQLException;

import com.aliyun.odps.data.Binary;

public class ToOdpsBinaryTransformer extends AbstractToOdpsTransformer {

  @Override
  public Object transform(Object o, String charset) throws SQLException {
    if (o == null) {
      return null;
    }

    if (Binary.class.isInstance(o)) {
      return o;
    } else if (String.class.isInstance(o)) {
      return new Binary(((String) o).getBytes());
    } else if (byte[].class.isInstance(o)) {
      return new Binary((byte[]) o);
    } else {
      String errorMsg = getInvalidTransformationErrorMsg(o.getClass(), Binary.class);
      throw new SQLException(errorMsg);
    }
  }
}
