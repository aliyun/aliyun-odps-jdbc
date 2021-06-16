package com.aliyun.odps.jdbc.utils.transformer.to.jdbc;

import com.aliyun.odps.jdbc.utils.JdbcColumn;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

public abstract class AbstractToJdbcDateTypeTransformer extends AbstractToJdbcTransformer {


  @Override
  public Object transform(Object o, String charset) throws SQLException {
    return this.transform(o, charset, null);
  }

  /**
   * Transform ODPS SDK object to an instance of java.util.Date subclass
   * @param o java object from ODPS SDK
   * @param charset charset to encode byte array
   * @param timeZone time zone. The default value is GMT +0.
   * @return JDBC object
   * @throws SQLException
   */
  public abstract Object transform(
      Object o,
      String charset,
      TimeZone timeZone) throws SQLException;
}
