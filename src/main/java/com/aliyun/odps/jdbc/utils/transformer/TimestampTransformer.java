package com.aliyun.odps.jdbc.utils.transformer;

import com.aliyun.odps.jdbc.utils.JdbcColumn;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class TimestampTransformer extends AbstractTransformer {

  @Override
  public Object transform(Object o, String charset) throws SQLException {
    if (o == null) {
      return null;
    }

    if (java.util.Date.class.isInstance(o)) {
      if (o instanceof java.sql.Timestamp) {
        return o;
      } else {
        // The following conversion will lose nano values
        return new java.sql.Timestamp(((java.util.Date) o).getTime());
      }
    } else if (o instanceof byte[]) {
      try {
        // Acceptable format: yyyy-[m]m-[d]d hh:mm:ss[.f...]
        return java.sql.Timestamp.valueOf(encodeBytes((byte[]) o, charset));
      } catch (IllegalArgumentException e) {
        String errorMsg = getTransformationErrMsg(o, java.sql.Timestamp.class);
        throw new SQLException(errorMsg);
      }
    } else {
      String errorMsg = getInvalidTransformationErrorMsg(o.getClass(), java.sql.Timestamp.class);
      throw new SQLException(errorMsg);
    }
  }
}
