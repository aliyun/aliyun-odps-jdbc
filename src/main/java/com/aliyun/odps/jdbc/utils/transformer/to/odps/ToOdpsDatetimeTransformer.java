package com.aliyun.odps.jdbc.utils.transformer.to.odps;

import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;

public class ToOdpsDatetimeTransformer extends AbstractToOdpsTransformer {

  @Override
  public Object transform(Object o, String charset) throws SQLException {
    if (o == null) {
      return null;
    }

    if (Timestamp.class.isInstance(o)
        || java.sql.Date.class.isInstance(o)
        || Time.class.isInstance(o)) {
      return new Date(((Date) o).getTime());
    } else if (Date.class.isInstance(o)) {
      return o;
    } else {
      String errorMsg = getInvalidTransformationErrorMsg(o.getClass(), Date.class);
      throw new SQLException(errorMsg);
    }
  }
}
