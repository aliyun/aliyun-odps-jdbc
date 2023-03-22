package com.aliyun.odps.jdbc.utils.transformer.to.odps;

import java.sql.SQLException;

import com.aliyun.odps.data.Char;

public class ToOdpsCharTransformer extends AbstractToOdpsTransformer {

  @Override
  public Object transform(Object o, String charset) throws SQLException {
    if (o == null) {
      return null;
    }

    if (Char.class.isInstance(o)) {
      return o;
    } else if (String.class.isInstance(o)) {
      return new Char((String) o);
    } else if (byte[].class.isInstance(o)) {
      return new Char(encodeBytes((byte[]) o, charset));
    } else {
      String errorMsg = getInvalidTransformationErrorMsg(o.getClass(), Char.class);
      throw new SQLException(errorMsg);
    }
  }
}
