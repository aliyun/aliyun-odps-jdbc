package com.aliyun.odps.jdbc.utils;

import java.sql.Date;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.TimeZone;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class TimeUtils {

  public static Date getDate(Date originDate, TimeZone oldTz, TimeZone newTz) throws SQLException {
    try {
      long millis = originDate.getTime();
      long milliSecsSinceEpochNew =
          millis + moveToTimeZoneOffset(millis, oldTz, newTz);
      return new Date(milliSecsSinceEpochNew);
    } catch (NumberFormatException ex) {
      throw new SQLException("Invalid date value: " + originDate);
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

    return offsetMillisInOldTZ - offsetMillisInNewTZ;
  }

}
