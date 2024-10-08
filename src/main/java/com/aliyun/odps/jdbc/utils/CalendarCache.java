package com.aliyun.odps.jdbc.utils;


import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.TimeZone;

public class CalendarCache {

  private static final ThreadLocal<CalendarCacheState> localState =
      ThreadLocal.withInitial(CalendarCacheState::new);

  public static GregorianCalendar get(TimeZone timezone, String tzId, String calId) {
    CalendarCacheState state = localState.get();
    if (state.enabled) {
      GregorianCalendar res = state.map.get(calId);
      if (res == null) {
        res = createCalendar(timezone, tzId);
        state.map.put(calId, res);
      } else {
        if (timezone != null && !res.getTimeZone().hasSameRules(timezone)) {
          throw new IllegalArgumentException("Cached timezone is not equivalent to the requested one:" + timezone + "  VS  " + res.getTimeZone());
        }

        if (state.alwaysGregorian && res.getGregorianChange().getTime() != Long.MIN_VALUE) {
          throw new IllegalArgumentException("Cached calendar gregorian offset is not set as expected:" + res.getGregorianChange().getTime() + "  VS  " + Long.MIN_VALUE);
        }
      }

      if (state.produceClones) {
        res = (GregorianCalendar)res.clone();
      } else {
        res.clear();
      }

      return res;
    } else {
      return createCalendar(timezone, tzId);
    }
  }

  private static GregorianCalendar createCalendar(TimeZone timezone, String tzId) {
    CalendarCacheState state = localState.get();
    if (timezone == null) {
      timezone = state.timeZoneFactory.getTimeZone(tzId);
    }

    GregorianCalendar cal = new GregorianCalendar(timezone);
    cal.clear();
    if (state.alwaysGregorian) {
      cal.setGregorianChange(new Date(Long.MIN_VALUE));
    }

    return cal;
  }

  public static GregorianCalendar get(TimeZone timezone, String calId) {
    return get(timezone, null, calId);
  }

  public static GregorianCalendar get(TimeZone timezone) {
    return get(timezone, null, timezone.getID());
  }

  public static GregorianCalendar get(String tzId) {
    return get(null, tzId, tzId);
  }

  private static class CalendarCacheState {
    HashMap<String, GregorianCalendar> map = new HashMap();
    boolean enabled = false;
    boolean produceClones = true;
    boolean alwaysGregorian = false;
    TimeZoneFactory timeZoneFactory = new DefaultTimeZoneFactory();

    CalendarCacheState() {
    }
  }

  private static class DefaultTimeZoneFactory implements TimeZoneFactory {
    private DefaultTimeZoneFactory() {
    }

    public TimeZone getTimeZone(String id) {
      return TimeZone.getTimeZone(id);
    }
  }

  public interface TimeZoneFactory {
    TimeZone getTimeZone(String var1);
  }
}

