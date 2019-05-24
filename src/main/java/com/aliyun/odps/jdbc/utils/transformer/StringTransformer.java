package com.aliyun.odps.jdbc.utils.transformer;

import com.aliyun.odps.jdbc.utils.JdbcColumn;
import java.sql.SQLException;
import java.text.SimpleDateFormat;

public class StringTransformer extends AbstractTransformer {

  @Override
  public Object transform(Object o, String charset) throws SQLException {
    if (o == null) {
      return null;
    }

    if (o instanceof byte[]) {
      return encodeBytes((byte[]) o, charset);
    } else if (java.util.Date.class.isInstance(o)) {
      if (java.sql.Timestamp.class.isInstance(o)) {
        return o.toString();
      }
      SimpleDateFormat dateFormat = new SimpleDateFormat(JdbcColumn.ODPS_DATETIME_FORMAT);
      return dateFormat.format(((java.util.Date) o));
    } else {
      return o.toString();
    }
  }
}
