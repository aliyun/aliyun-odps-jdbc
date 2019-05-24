package com.aliyun.odps.jdbc.utils.transformer;

import com.aliyun.odps.jdbc.utils.JdbcColumn;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class DateTransformer extends AbstractTransformer {

  @Override
  public Object transform(Object o, String charset) throws SQLException {
    if (o == null) {
      return null;
    }

    if (java.util.Date.class.isInstance(o)) {
      return new java.sql.Date(((java.util.Date) o).getTime());
    } else if (o instanceof byte[]) {
      SimpleDateFormat dateFormat = new SimpleDateFormat(JdbcColumn.ODPS_DATETIME_FORMAT);
      try {
        return new java.sql.Date(dateFormat.parse(encodeBytes((byte[]) o, charset)).getTime());
      } catch (ParseException e) {
        String errorMsg =
            getTransformationErrMsg(encodeBytes((byte[]) o, charset), java.sql.Date.class);
        throw new SQLException(errorMsg);
      }
    } else {
      String errorMsg = getInvalidTransformationErrorMsg(o.getClass(), java.sql.Date.class);
      throw new SQLException(errorMsg);
    }
  }
}
