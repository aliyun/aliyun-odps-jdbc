package com.aliyun.odps.jdbc.utils;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.TimeZone;


/**
 * <h1>JDBC Time Zone Logic Description</h1>
 *
 * <p>
 * The default time zone used by JDBC is UTC. This is generally sufficient in most cases,
 * as Java's {@link java.sql.Timestamp} and {@link java.util.Date} types will print using the local time zone.
 * </p>
 *
 * <p>
 * However, we can modify the time zone behavior of JDBC using the following methods:
 * </p>
 *
 * <ol>
 *   <li>
 *     <strong>timezone parameter:</strong>
 *     <p>
 *     This parameter has the highest priority. If the timezone parameter is specified in the connection string,
 *     JDBC will use that time zone. Additionally, this parameter will also configure the time zone used
 *     for executing SQL on the server side.
 *     </p>
 *   </li>
 *   <li>
 *     <strong>useProjectTimeZone parameter:</strong>
 *     <p>
 *     This parameter has a lower priority than the timezone parameter. If the timezone parameter is not specified,
 *     JDBC will attempt to retrieve the time zone configuration from the {@link  com.aliyun.odps.Project}
 *     and use that project's time zone.
 *     </p>
 *   </li>
 * </ol>
 *
 * <p>
 * By configuring these parameters appropriately, users can flexibly manage the JDBC time zone settings
 * according to business requirements, ensuring the consistency and accuracy of time data.
 * </p>
 *
 * @see java.sql.Timestamp
 * @see java.util.Date
 * @see com.aliyun.odps.Project
 */
public class TimeUtils {

  public static final TimeZone UTC = TimeZone.getTimeZone("UTC");

  public static Date getDate(LocalDate originDate, TimeZone targetTimezone) throws SQLException {
    try {
      long millis = originDate.atStartOfDay().toEpochSecond(ZoneOffset.UTC) * 1000;
      return new Date(millis);
    } catch (NumberFormatException ex) {
      throw new SQLException("Invalid date value: " + originDate);
    }
  }


  public static Date getDate(LocalDateTime originDate, TimeZone targetTimezone) throws SQLException {
    try {
      long millis = originDate.toEpochSecond(ZoneOffset.UTC) * 1000 + originDate.getNano() / 1000000;
      long milliSecsSinceEpochNew =
          millis + moveToTimeZoneOffset(millis, TimeZone.getDefault(), UTC);
      return new Date(milliSecsSinceEpochNew);
    } catch (NumberFormatException ex) {
      throw new SQLException("Invalid date value: " + originDate);
    }
  }

  public static Date getDate(Instant originDate, TimeZone targetTimezone) throws SQLException {
    try {
      long millis = originDate.getEpochSecond() * 1000 + originDate.getNano() / 1000000;
      return new Date(millis);
    } catch (NumberFormatException ex) {
      throw new SQLException("Invalid date value: " + originDate);
    }
  }

  public static Timestamp getTimestamp(Timestamp originTimestamp, TimeZone targetTimezone) throws SQLException {
    try {
      long millis = originTimestamp.getTime();
      long milliSecsSinceEpochNew =
          millis + moveToTimeZoneOffset(millis, TimeZone.getDefault(), targetTimezone);
      Timestamp res = new Timestamp(milliSecsSinceEpochNew);
      res.setNanos(originTimestamp.getNanos());
      return res;
    } catch (NumberFormatException ex) {
      throw new SQLException("Invalid date value: " + originTimestamp);
    }
  }

  public static Timestamp getTimestamp(LocalDateTime originTimestamp, TimeZone targetTimezone) throws SQLException {
    try {
      long millis = originTimestamp.toEpochSecond(ZoneOffset.UTC) * 1000 + originTimestamp.getNano() / 1000000;
      long milliSecsSinceEpochNew =
          millis + moveToTimeZoneOffset(millis, TimeZone.getDefault(), targetTimezone);
      Timestamp res = new Timestamp(milliSecsSinceEpochNew);
      res.setNanos(originTimestamp.getNano());
      return res;
    } catch (NumberFormatException ex) {
      throw new SQLException("Invalid date value: " + originTimestamp);
    }
  }

  public static Timestamp getTimestamp(Instant originTimestamp, TimeZone targetTimezone) throws SQLException {
    try {
      long millis = originTimestamp.getEpochSecond() * 1000 + originTimestamp.getNano() / 1000000;
      long milliSecsSinceEpochNew =
          millis + moveToTimeZoneOffset(millis, TimeZone.getDefault(), targetTimezone);
      Timestamp res = new Timestamp(milliSecsSinceEpochNew);
      res.setNanos(originTimestamp.getNano());
      return res;
    } catch (NumberFormatException ex) {
      throw new SQLException("Invalid date value: " + originTimestamp);
    }
  }

  public static Time getTime(LocalDateTime originTimestamp, TimeZone targetTimezone) throws SQLException {
    try {
      long millis = originTimestamp.toEpochSecond(ZoneOffset.UTC) * 1000 + originTimestamp.getNano() / 1000000;
      long milliSecsSinceEpochNew =
          millis + moveToTimeZoneOffset(millis, TimeZone.getDefault(), targetTimezone);
      return new Time(milliSecsSinceEpochNew);
    } catch (NumberFormatException ex) {
      throw new SQLException("Invalid date value: " + originTimestamp);
    }
  }

  public static Time getTime(Instant originTimestamp, TimeZone targetTimezone) throws SQLException {
    try {
      long millis = originTimestamp.getEpochSecond() * 1000 + originTimestamp.getNano() / 1000000;
      long milliSecsSinceEpochNew =
          millis + moveToTimeZoneOffset(millis, TimeZone.getDefault(), targetTimezone);
      return new Time(milliSecsSinceEpochNew);
    } catch (NumberFormatException ex) {
      throw new SQLException("Invalid date value: " + originTimestamp);
    }
  }

  /**
   * simplified moveToTimeZone method
   *
   * @param milliSecsSinceEpoch
   * @param oldTZ
   * @param newTZ
   * @return offset
   */
  private static long moveToTimeZoneOffset(
      long milliSecsSinceEpoch, TimeZone oldTZ, TimeZone newTZ) {
    if (oldTZ.hasSameRules(newTZ)) {
      // same time zone
      return 0;
    }
    int offsetMillisInOldTZ = oldTZ.getOffset(milliSecsSinceEpoch);

    Calendar calendar = CalendarCache.get(oldTZ);
    calendar.setTimeInMillis(milliSecsSinceEpoch);

    int millisecondWithinDay =
        ((calendar.get(Calendar.HOUR_OF_DAY) * 60 + calendar.get(Calendar.MINUTE)) * 60
         + calendar.get(Calendar.SECOND))
        * 1000
        + calendar.get(Calendar.MILLISECOND);

    int era = calendar.get(Calendar.ERA);
    int year = calendar.get(Calendar.YEAR);
    int month = calendar.get(Calendar.MONTH);
    int dayOfMonth = calendar.get(Calendar.DAY_OF_MONTH);
    int dayOfWeek = calendar.get(Calendar.DAY_OF_WEEK);

    int offsetMillisInNewTZ =
        newTZ.getOffset(era, year, month, dayOfMonth, dayOfWeek, millisecondWithinDay);

    return offsetMillisInNewTZ - offsetMillisInOldTZ;
  }
}
