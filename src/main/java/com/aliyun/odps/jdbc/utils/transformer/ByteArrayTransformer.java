package com.aliyun.odps.jdbc.utils.transformer;

import com.aliyun.odps.jdbc.utils.JdbcColumn;
import java.sql.SQLException;
import java.text.SimpleDateFormat;

public class ByteArrayTransformer extends AbstractTransformer {

  @Override
  public Object transform(Object o, String charset) throws SQLException {
    if (o == null) {
      return null;
    }

    if (o instanceof byte[]) {
      return o;
    } else if (java.util.Date.class.isInstance(o)) {
      if (java.sql.Timestamp.class.isInstance(o)) {
        return o.toString().getBytes();
      }
      SimpleDateFormat dateFormat = new SimpleDateFormat(JdbcColumn.ODPS_DATETIME_FORMAT);
      return dateFormat.format(((java.util.Date) o)).getBytes();
    } else {
      return o.toString().getBytes();
    }
  }
}
