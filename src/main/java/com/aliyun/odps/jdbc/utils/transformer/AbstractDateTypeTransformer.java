package com.aliyun.odps.jdbc.utils.transformer;

import java.sql.SQLException;
import java.util.Calendar;
import java.util.TimeZone;

public abstract class AbstractDateTypeTransformer extends AbstractTransformer {

  @Override
  public Object transform(Object o, String charset) throws SQLException {
    return this.transform(o, charset, null);
  }

  /**
   * Transform ODPS SDK object to an instance of java.util.Date subclass
   * @param o java object from ODPS SDK
   * @param charset charset to encode byte array
   * @param cal a calendar object to construct java.util.Date object
   * @return JDBC object
   * @throws SQLException
   */
  public abstract Object transform(Object o, String charset, Calendar cal) throws SQLException;

  /**
   * Return the timezone of the input calendar
   * @param cal a calendar object
   * @return the timezone of the input calendar. If cal == null, or the cal.getTimezone() == null,
   * return the timezone of this JVM
   */
  TimeZone getTimeZone(Calendar cal) {
    if (cal == null || cal.getTimeZone() == null) {
      return TimeZone.getDefault();
    }
    return cal.getTimeZone();
  }
}
