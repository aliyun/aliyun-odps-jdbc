package com.aliyun.odps.jdbc.utils.transformer.to.jdbc;

import com.aliyun.odps.jdbc.utils.JdbcColumn;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

public abstract class AbstractToJdbcDateTypeTransformer extends AbstractToJdbcTransformer {
  static ThreadLocal<SimpleDateFormat> DATETIME_FORMAT = new ThreadLocal<>();
  static ThreadLocal<SimpleDateFormat> DATE_FORMAT = new ThreadLocal<>();
  static ThreadLocal<SimpleDateFormat> TIME_FORMAT = new ThreadLocal<>();
  static {
    DATETIME_FORMAT.set(new SimpleDateFormat(JdbcColumn.ODPS_DATETIME_FORMAT));
    DATE_FORMAT.set(new SimpleDateFormat(JdbcColumn.ODPS_DATE_FORMAT));
    TIME_FORMAT.set(new SimpleDateFormat(JdbcColumn.ODPS_TIME_FORMAT));
  }

  @Override
  public Object transform(Object o, String charset) throws SQLException {
    return this.transform(o, charset, null, null);
  }

  /**
   * Transform ODPS SDK object to an instance of java.util.Date subclass
   * @param o java object from ODPS SDK
   * @param charset charset to encode byte array
   * @param cal a calendar object to construct java.util.Date object
   * @return JDBC object
   * @throws SQLException
   */
  public abstract Object transform(
      Object o,
      String charset,
      Calendar cal,
      TimeZone projectTimeZone) throws SQLException;

  void restoreToDefaultCalendar() {
    DATETIME_FORMAT.get().setCalendar(Calendar.getInstance());
    DATE_FORMAT.get().setCalendar(Calendar.getInstance());
    TIME_FORMAT.get().setCalendar(Calendar.getInstance());
  }
}
