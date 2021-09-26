package com.aliyun.odps.jdbc.utils.transformer.to.jdbc;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

import com.aliyun.odps.jdbc.utils.JdbcColumn;
import com.aliyun.odps.type.TypeInfo;

public abstract class AbstractToJdbcDateTypeTransformer extends AbstractToJdbcTransformer {

  static ThreadLocal<Calendar>
      DEFAULT_CALENDAR =
      ThreadLocal.withInitial(() -> new Calendar.Builder().setCalendarType("iso8601")
          .setTimeZone(TimeZone.getTimeZone("GMT")).build());
  static ThreadLocal<SimpleDateFormat>
      TIMESTAMP_FORMAT =
      ThreadLocal.withInitial(() -> new SimpleDateFormat(JdbcColumn.ODPS_TIMESTAMP_FORMAT));
  static ThreadLocal<SimpleDateFormat>
      DATETIME_FORMAT =
      ThreadLocal.withInitial(() -> new SimpleDateFormat(JdbcColumn.ODPS_DATETIME_FORMAT));
  static ThreadLocal<SimpleDateFormat>
      DATE_FORMAT =
      ThreadLocal.withInitial(() -> new SimpleDateFormat(JdbcColumn.ODPS_DATE_FORMAT));
  static ThreadLocal<SimpleDateFormat>
      TIME_FORMAT =
      ThreadLocal.withInitial(() -> new SimpleDateFormat(JdbcColumn.ODPS_TIME_FORMAT));

  @Override
  public Object transform(Object o, String charset) throws SQLException {
    return this.transform(o, charset, null, null);
  }

  @Override
  public Object transform(Object o, String charset, TypeInfo odpsType) throws SQLException {
    return this.transform(o, charset, null, null, odpsType);
  }

  /**
   * Transform ODPS SDK object to an instance of java.util.Date subclass
   *
   * @param o       java object from ODPS SDK
   * @param charset charset to encode byte array
   * @param cal     a calendar object to construct java.util.Date object
   * @return JDBC object
   * @throws SQLException
   */
  public abstract Object transform(
      Object o,
      String charset,
      Calendar cal,
      TimeZone timeZone) throws SQLException;

  public Object transform(
      Object o,
      String charset,
      Calendar cal,
      TimeZone timeZone,
      TypeInfo odpsType) throws SQLException {
    //default implement
    return transform(o, charset, cal, timeZone);
  }

  void restoreToDefaultCalendar() {
    TIMESTAMP_FORMAT.get().setCalendar(Calendar.getInstance());
    DATETIME_FORMAT.get().setCalendar(Calendar.getInstance());
    DATE_FORMAT.get().setCalendar(Calendar.getInstance());
    TIME_FORMAT.get().setCalendar(Calendar.getInstance());
  }
}