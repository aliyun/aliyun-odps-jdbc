package com.aliyun.odps.jdbc.time;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.TimeZone;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aliyun.odps.jdbc.utils.TestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cross contract for DATE / DATETIME / TIMESTAMP / TIMESTAMP_NTZ over
 * (session timezone x JVM default timezone x Calendar argument x read path x write path).
 *
 * <p>Expected values are derived from the fixed input with {@code java.time} and the JDK's own
 * {@code Timestamp.valueOf} -- never by calling {@code JdbcTimeUtil} or a transformer. A suite that
 * computed its expectations from the code under test could not have found the two defects this
 * suite pins. Both were measured on a real project first, over a 216-observation matrix
 * (3 session zones x 3 JVM zones x 6 rows x 4 columns); the work item's evidence holds those
 * transcripts.
 *
 * <p>Rules:
 * <ul>
 *   <li><b>DATETIME / TIMESTAMP</b> hold an absolute instant. The driver prints that instant as the
 *       clock of the session timezone, so {@code getTimestamp(i).toString()} is the clock the
 *       server prints and {@code getTime()} is only the true instant while the JVM offset matches
 *       the session offset. That display convention is deliberate and untouched here.</li>
 *   <li><b>TIMESTAMP_NTZ</b> holds a clock, not an instant: no session offset may be applied to it.</li>
 *   <li>A <b>Calendar</b> says which zone the printed clock belongs to -- the same rule the
 *       character-column path has always used. A Calendar matching the session zone changes nothing.</li>
 *   <li><b>DATE</b> holds a calendar date: no zone, no clock.</li>
 * </ul>
 */
public class OdpsJdbcTimezoneContractTest {

  static final String UTC = "UTC";
  static final String SHANGHAI = "Asia/Shanghai";
  static final String LOS_ANGELES = "America/Los_Angeles";
  static final String TOKYO = "Asia/Tokyo";
  static final String SANTIAGO = "America/Santiago";

  static final DateTimeFormatter NINE =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");
  static final DateTimeFormatter SECONDS =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

  static final String TABLE = "odps_jdbc_timezone_contract";
  /**
   * Same columns minus TIMESTAMP_NTZ. A NTZ column may not appear in a tunnel-path write at all
   * (see {@link #ntzCannotBeBoundThroughPreparedStatement()}), so the write-side contract needs a
   * table the tunnel path can actually open.
   */
  static final String WRITE_TABLE = "odps_jdbc_timezone_contract_w";

  // seeded wall clocks, all written while the session timezone was UTC
  static final String NANOS = "nanos";
  static final String PRE_EPOCH = "preEpoch";
  static final String FIRST_NANO = "firstNano";
  static final String DAY_LINE = "dayLine";
  static final String SH_GAP = "shanghaiGap1986";
  static final String OV_EARLY = "overlapEarly";
  static final String OV_LATE = "overlapLate";

  static final LocalDateTime NANOS_CLOCK = LocalDateTime.parse("2026-07-15T12:34:56.123456789");
  static final LocalDateTime PRE_EPOCH_CLOCK = LocalDateTime.parse("1969-12-31T23:59:59.123456789");
  static final LocalDateTime FIRST_NANO_CLOCK =
      LocalDateTime.parse("1970-01-01T00:00:00.000000001");
  /** 2025-12-31T16:30Z reads as 2026-01-01T00:30 at UTC+8 and 2025-12-31T08:30 at UTC-8. */
  static final LocalDateTime DAY_LINE_CLOCK = LocalDateTime.parse("2025-12-31T16:30:00.000000001");
  /** China ran DST from 1986-05-04 02:00, so that Shanghai clock never existed. */
  static final LocalDateTime SH_GAP_CLOCK = LocalDateTime.parse("1986-05-04T02:30:00");
  /** Two instants an hour apart that are one identical clock in Los Angeles on 2026-11-01. */
  static final LocalDateTime OV_EARLY_CLOCK = LocalDateTime.parse("2026-11-01T08:30:00");
  static final LocalDateTime OV_LATE_CLOCK = LocalDateTime.parse("2026-11-01T09:30:00");

  static Connection seeded;

  @BeforeAll
  static void seedThroughServer() throws Exception {
    TimeZone.setDefault(TimeZone.getTimeZone(UTC));
    seeded = TestUtils.getConnectionWithTimezone(UTC);
    try (Statement st = seeded.createStatement()) {
      st.execute("set odps.sql.type.system.odps2=true;");
      st.execute("drop table if exists " + TABLE);
      st.execute("drop table if exists " + WRITE_TABLE);
      st.execute("create table " + TABLE
                 + " (k string, d date, dt datetime, ts timestamp, tsntz timestamp_ntz)");
      st.execute("create table " + WRITE_TABLE
                 + " (k string, d date, dt datetime, ts timestamp)");
      st.execute("insert into " + TABLE + " values "
                 + row(NANOS, NANOS_CLOCK) + ", " + row(PRE_EPOCH, PRE_EPOCH_CLOCK) + ", "
                 + row(FIRST_NANO, FIRST_NANO_CLOCK) + ", " + row(DAY_LINE, DAY_LINE_CLOCK) + ", "
                 + row(SH_GAP, SH_GAP_CLOCK) + ", " + row(OV_EARLY, OV_EARLY_CLOCK) + ", "
                 + row(OV_LATE, OV_LATE_CLOCK));
    }
  }

  /**
   * One literal row. The DATETIME column gets no fraction: MaxCompute refuses a fractional seconds
   * part in a {@code DATETIME'} literal outright (ODPS-0130161), which is itself pinned by
   * {@link #subSecondPrecisionRules()}.
   */
  static String row(String key, LocalDateTime clock) {
    return String.format("('%s', date'%s', datetime'%s', timestamp'%s', timestamp_ntz'%s')",
                         key, clock.toLocalDate(), clock.withNano(0).format(SECONDS),
                         clock.format(NINE), clock.format(NINE));
  }

  @AfterAll
  static void dropSeededRows() throws Exception {
    if (seeded != null) {
      try (Statement st = seeded.createStatement()) {
        st.execute("drop table if exists " + TABLE);
        st.execute("drop table if exists " + WRITE_TABLE);
      }
      seeded.close();
    }
    // the rest of this package assumes Asia/Shanghai as the process default
    TimeZone.setDefault(TimeZone.getTimeZone(SHANGHAI));
  }

  // ---------------------------------------------------------------- expectation helpers

  static ZoneId zone(String id) {
    return ZoneId.of(id);
  }

  /** the instant the server holds for a clock written under a UTC session. */
  static Instant storedInstant(LocalDateTime clock) {
    return clock.atZone(zone(UTC)).toInstant();
  }

  /** the clock the driver shows for an absolute value under session timezone {@code session}. */
  static LocalDateTime printedClock(Instant instant, String session) {
    return instant.atZone(zone(session)).toLocalDateTime();
  }

  /**
   * The clock after a Calendar has claimed it: the printed clock is re-read as a clock of the
   * Calendar's zone and shown in the session zone again. Re-deriving it here, in the test, is the
   * point -- it is the rule the character-column path already implements.
   */
  static LocalDateTime byCalendar(LocalDateTime printed, String session, String calendarZone) {
    return printed.atZone(zone(calendarZone)).withZoneSameInstant(zone(session)).toLocalDateTime();
  }

  static Calendar cal(String zoneId) {
    Calendar c = Calendar.getInstance();
    c.setTimeZone(TimeZone.getTimeZone(zoneId));
    return c;
  }

  /** the server's own rendering of a column; its width tracks the fraction, so parse it. */
  static LocalDateTime serverClock(ResultSet rs, int column) throws SQLException {
    String text = rs.getString(column);
    return text == null ? null : LocalDateTime.parse(text.replace(' ', 'T'));
  }

  interface RowBody {
    void row(ResultSet rs) throws Exception;
  }

  /** Runs one query under one session timezone and one JVM default timezone. */
  static void under(String session, String jvm, RowBody body) throws Exception {
    TimeZone previous = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone(jvm));
    try (Connection c = TestUtils.getConnectionWithTimezone(session);
         Statement st = c.createStatement()) {
      st.execute("set odps.sql.type.system.odps2=true;");
      try (ResultSet rs = st.executeQuery("select k, d, dt, ts, tsntz from " + TABLE + ";")) {
        while (rs.next()) {
          body.row(rs);
        }
      }
    } finally {
      TimeZone.setDefault(previous);
    }
  }

  /** only these rows are seeded by {@link #seedThroughServer()}; the write-side tests add others. */
  static final java.util.Set<String> SEEDED = new java.util.HashSet<>(
      java.util.Arrays.asList(NANOS, PRE_EPOCH, FIRST_NANO, DAY_LINE, SH_GAP, OV_EARLY, OV_LATE));

  static LocalDateTime clockOf(String key) {
    switch (key) {
      case NANOS:
        return NANOS_CLOCK;
      case PRE_EPOCH:
        return PRE_EPOCH_CLOCK;
      case FIRST_NANO:
        return FIRST_NANO_CLOCK;
      case DAY_LINE:
        return DAY_LINE_CLOCK;
      case SH_GAP:
        return SH_GAP_CLOCK;
      case OV_EARLY:
        return OV_EARLY_CLOCK;
      case OV_LATE:
        return OV_LATE_CLOCK;
      default:
        throw new IllegalArgumentException("unseeded row " + key);
    }
  }

  // ------------------------------------------------------- A1 / A2: absolute columns

  /**
   * The absolute columns, all nine (session, JVM) combinations: the raw value is the instant, the
   * server's text and the driver's display are the session clock, and the {@code Timestamp} carries
   * that clock anchored in the JVM zone.
   */
  @Test
  public void absoluteColumnsAreInstantInRawFormAndSessionClockInTextForm() throws Exception {
    for (String session : new String[] {UTC, SHANGHAI, LOS_ANGELES}) {
      for (String jvm : new String[] {UTC, SHANGHAI, LOS_ANGELES}) {
        under(session, jvm, rs -> {
          String key = rs.getString(1);
          if (!SEEDED.contains(key)) {
            return;
          }
          LocalDateTime clock = clockOf(key);
          Instant tsInstant = storedInstant(clock);
          Instant dtInstant = storedInstant(clock.withNano(0));

          assertEquals(Instant.class, rs.getObject(4).getClass(), key + " raw TIMESTAMP type");
          assertEquals(tsInstant, rs.getObject(4), key + "/" + session + " raw TIMESTAMP instant");
          assertEquals(ZonedDateTime.class, rs.getObject(3).getClass(), key + " raw DATETIME type");
          assertEquals(dtInstant, ((ZonedDateTime) rs.getObject(3)).toInstant(),
                       key + "/" + session + " raw DATETIME instant");

          assertEquals(printedClock(tsInstant, session), serverClock(rs, 4),
                       key + "/" + session + " server text of TIMESTAMP");
          assertEquals(printedClock(dtInstant, session), serverClock(rs, 3),
                       key + "/" + session + " server text of DATETIME");

          Timestamp ts = rs.getTimestamp(4);
          Timestamp dt = rs.getTimestamp(3);
          // expectedTimestamp is the JDK's rule (clock anchored in the JVM default zone), which is
          // what this driver's display convention produces
          assertEquals(Timestamp.valueOf(printedClock(tsInstant, session)), ts,
                       key + "/" + session + "/" + jvm + " getTimestamp(TIMESTAMP)");
          assertEquals(Timestamp.valueOf(printedClock(dtInstant, session)), dt,
                       key + "/" + session + "/" + jvm + " getTimestamp(DATETIME)");
          assertEquals(clock.getNano(), ts.getNanos(), key + " all nine TIMESTAMP digits survive");
          assertEquals(0, dt.getNanos(), key + " DATETIME as written has no fraction");
          assertEquals(printedClock(tsInstant, session).toLocalDate(), rs.getDate(4).toLocalDate(),
                       key + "/" + session + " getDate(TIMESTAMP) follows the session zone");
        });
      }
    }
  }

  // ---------------------------------------------------------------- A2: TIMESTAMP_NTZ

  /**
   * TIMESTAMP_NTZ names no timezone, so the stored clock is the clock the caller gets -- through
   * every accessor, in every session zone. Before the fix the session offset was applied to it:
   * the value moved by hours, and a clock near midnight moved by a calendar day, while
   * {@code getObject} and the server's own text of the same column said something else.
   */
  @Test
  public void ntzClockIsTheSameThroughEveryAccessor() throws Exception {
    for (String session : new String[] {UTC, SHANGHAI, LOS_ANGELES, SANTIAGO}) {
      under(session, UTC, rs -> {
        String key = rs.getString(1);
        LocalDateTime clock = clockOf(key);
        assertEquals(LocalDateTime.class, rs.getObject(5).getClass(), key + " raw NTZ type");
        assertEquals(clock, rs.getObject(5), key + "/" + session + " raw NTZ clock");
        assertEquals(clock, serverClock(rs, 5),
                     key + "/" + session + " the server never moves an NTZ clock either");
        assertEquals(Timestamp.valueOf(clock), rs.getTimestamp(5),
                     key + "/" + session + " getTimestamp(NTZ) keeps the clock");
        assertEquals(clock.getNano(), rs.getTimestamp(5).getNanos(),
                     key + "/" + session + " getTimestamp(NTZ) keeps the nanoseconds");
        assertEquals(Date.valueOf(clock.toLocalDate()), rs.getDate(5),
                     key + "/" + session + " getDate(NTZ) keeps the date, no day-line crossing");
        assertEquals(LocalTime.from(clock.withNano(0)), rs.getTime(5).toLocalTime(),
                     key + "/" + session + " getTime(NTZ) keeps the time");
      });
    }
  }

  /**
   * Same row, two columns: the absolute TIMESTAMP date follows the session zone across the day
   * line, the NTZ copy of the same clock must not.
   */
  @Test
  public void onlyTheAbsoluteColumnCrossesTheDayLine() throws Exception {
    for (String session : new String[] {UTC, SHANGHAI, LOS_ANGELES}) {
      under(session, UTC, rs -> {
        if (!DAY_LINE.equals(rs.getString(1))) {
          return;
        }
        assertEquals(printedClock(storedInstant(DAY_LINE_CLOCK), session).toLocalDate(),
                     rs.getDate(4).toLocalDate(), session + " TIMESTAMP date follows the zone");
        assertEquals(LocalDate.parse("2025-12-31"), rs.getDate(5).toLocalDate(),
                     session + " NTZ date stays where it was written");
        assertEquals(LocalDate.parse("2025-12-31"), rs.getDate(2).toLocalDate(),
                     session + " DATE is a calendar date and cannot move");
      });
    }
  }

  /**
   * A DATE is a calendar date: session zone, JVM zone and Calendar argument all leave it alone.
   */
  @Test
  public void dateColumnIgnoresEveryZone() throws Exception {
    for (String session : new String[] {UTC, SHANGHAI, LOS_ANGELES}) {
      under(session, UTC, rs -> {
        if (!SEEDED.contains(rs.getString(1))) {
          return;
        }
        LocalDate expected = clockOf(rs.getString(1)).toLocalDate();
        assertEquals(expected, rs.getObject(2), rs.getString(1) + " raw DATE");
        assertEquals(Date.valueOf(expected), rs.getDate(2), rs.getString(1) + " getDate");
        assertEquals(Date.valueOf(expected), rs.getDate(2, cal(TOKYO)),
                     rs.getString(1) + " a Calendar cannot move a date");
      });
    }
  }

  /**
   * A DATE column has no clock to hand out, and the driver says so instead of inventing midnight.
   */
  @Test
  public void dateColumnRefusesToPretendItHasAClock() throws Exception {
    under(UTC, UTC, rs -> {
      String key = rs.getString(1);
      if (!SEEDED.contains(key)) {
        return;
      }
      SQLException ts = assertThrows(SQLException.class, () -> rs.getTimestamp(2),
                                     key + " getTimestamp(DATE) must not guess");
      assertTrue(ts.getMessage().contains("Cannot transform"), ts.getMessage());
      assertThrows(SQLException.class, () -> rs.getTime(2), key + " getTime(DATE) must not guess");
    });
  }

  // ---------------------------------------------------------------- A2: Calendar argument

  /**
   * A Calendar on a native date/time column must mean what it already meant on a character column:
   * the printed clock belongs to the Calendar's zone. A Calendar equal to the session zone changes
   * nothing, so callers that hand one over defensively are not disturbed. Before the fix the
   * argument was accepted and dropped, and {@code getTimestamp(i, cal)} returned exactly what
   * {@code getTimestamp(i)} did.
   */
  @Test
  public void calendarSaysWhichZoneThePrintedClockBelongsTo() throws Exception {
    for (String session : new String[] {UTC, SHANGHAI}) {
      under(session, UTC, rs -> {
        String key = rs.getString(1);
        if (!SEEDED.contains(key)) {
          return;
        }
        LocalDateTime clock = clockOf(key);
        LocalDateTime shown = printedClock(storedInstant(clock), session);
        LocalDateTime shownDateTime = printedClock(storedInstant(clock.withNano(0)), session);
        for (String z : new String[] {session, UTC, TOKYO, LOS_ANGELES}) {
          assertEquals(Timestamp.valueOf(byCalendar(shown, session, z)),
                       rs.getTimestamp(4, cal(z)), key + "/" + session + " TIMESTAMP " + z);
          assertEquals(Timestamp.valueOf(byCalendar(shownDateTime, session, z)),
                       rs.getTimestamp(3, cal(z)), key + "/" + session + " DATETIME " + z);
          // an NTZ clock ignores the *session* zone (it has none) but a Calendar is an explicit
          // statement about the clock's own zone, so it applies here as it does everywhere else
          assertEquals(Timestamp.valueOf(byCalendar(clock, session, z)),
                       rs.getTimestamp(5, cal(z)), key + "/" + session + " NTZ " + z);
          assertEquals(Date.valueOf(byCalendar(shown, session, z).toLocalDate()),
                       rs.getDate(4, cal(z)), key + "/" + session + " getDate(TIMESTAMP) " + z);
          if (session.equals(z)) {
            assertEquals(rs.getTimestamp(4), rs.getTimestamp(4, cal(z)),
                         "a session-equal Calendar changes nothing, TIMESTAMP");
            assertEquals(rs.getDate(4), rs.getDate(4, cal(z)),
                         "a session-equal Calendar changes nothing, getDate");
          }
        }
      });
    }
  }

  /**
   * The reference the native column has to match: the same clock handed back as text, read through
   * the character path where the Calendar has always worked. Agreement between the two is what
   * "consistent" means in this driver.
   */
  @Test
  public void nativeColumnAndItsOwnTextFormAgreeUnderACalendar() throws Exception {
    for (String session : new String[] {UTC, SHANGHAI}) {
      TimeZone previous = TimeZone.getDefault();
      TimeZone.setDefault(TimeZone.getTimeZone(UTC));
      try (Connection c = TestUtils.getConnectionWithTimezone(session);
           Statement st = c.createStatement()) {
        st.execute("set odps.sql.type.system.odps2=true;");
        try (ResultSet rs = st.executeQuery(
            "select cast(ts as string) text_form, ts, tsntz from " + TABLE + " where k='" + NANOS
            + "';")) {
          assertTrue(rs.next());
          for (String z : new String[] {UTC, TOKYO, LOS_ANGELES, session}) {
            assertEquals(rs.getTimestamp(1, cal(z)), rs.getTimestamp(2, cal(z)),
                         session + " TIMESTAMP column vs its own text form, Calendar " + z);
          }
        }
      } finally {
        TimeZone.setDefault(previous);
      }
    }
  }

  // ---------------------------------------------------------------- A1: DST gap and overlap

  /**
   * A clock inside a DST gap. Asia/Shanghai went from 02:00 to 03:00 on 1986-05-04, so 02:30 never
   * existed there. The server resolves it once, {@code java.time} resolves it once, and
   * {@code java.sql.Timestamp} cannot represent it at all -- pinned so that a change in any of the
   * three is visible instead of silently moving an hour of data.
   */
  @Test
  public void gapClockIsResolvedByTheZonesRules() throws Exception {
    assertEquals(LocalDateTime.parse("1986-05-04T03:30"),
                 SH_GAP_CLOCK.atZone(zone(SHANGHAI)).toLocalDateTime(),
                 "the expectation itself, from the tz rules: Shanghai 1986-05-04 02:30 becomes 03:30");

    // the server agrees Shanghai was UTC+9 that summer
    under(SHANGHAI, UTC, rs -> {
      if (!SH_GAP.equals(rs.getString(1))) {
        return;
      }
      assertEquals(LocalDateTime.parse("1986-05-04T11:30"), serverClock(rs, 3),
                   "DATETIME text carries Shanghai's 1986 DST offset, not +08");
      assertEquals(SH_GAP_CLOCK, rs.getObject(5), "the NTZ copy stays the clock it was written as");
    });

    // a JVM whose zone has the gap: the JDK anchors the clock past it, the driver adds nothing
    under(UTC, SHANGHAI, rs -> {
      if (!SH_GAP.equals(rs.getString(1))) {
        return;
      }
      Timestamp ntz = rs.getTimestamp(5);
      assertEquals(SH_GAP_CLOCK, rs.getObject(5), "raw NTZ clock untouched by any gap");
      assertEquals(LocalDateTime.parse("1986-05-04T03:30"), ntz.toLocalDateTime(),
                   "java.sql.Timestamp anchors a JVM-zone gap clock past the gap");
      assertEquals(SH_GAP_CLOCK.getNano(), ntz.getNanos(), "the fraction still belongs to the clock");
    });

    // a JVM zone with no gap reads the same value back exactly
    under(UTC, UTC, rs -> {
      if (!SH_GAP.equals(rs.getString(1))) {
        return;
      }
      assertEquals(SH_GAP_CLOCK, rs.getTimestamp(5).toLocalDateTime(), "no gap, no shift");
    });
  }

  /**
   * The fall-back hour: two instants an hour apart that are one identical clock in Los Angeles.
   * Read as an absolute value they stay apart; read as the driver prints them they collapse. An NTZ
   * column never held two instants in the first place, so it stays distinct.
   */
  @Test
  public void fallBackHourCollapsesThePrintedClockButNotTheInstant() throws Exception {
    final LocalDateTime[] instants = new LocalDateTime[2];
    final LocalDateTime[] shown = new LocalDateTime[2];
    final LocalDateTime[] ntz = new LocalDateTime[2];
    final int[] seen = {0};
    under(LOS_ANGELES, UTC, rs -> {
      String key = rs.getString(1);
      if (!key.startsWith("overlap")) {
        return;
      }
      int at = OV_EARLY.equals(key) ? 0 : 1;
      instants[at] = ((Instant) rs.getObject(4)).atZone(zone(UTC)).toLocalDateTime();
      shown[at] = rs.getTimestamp(4).toLocalDateTime();
      ntz[at] = (LocalDateTime) rs.getObject(5);
      seen[0]++;
    });
    assertEquals(2, seen[0], "both fall-back rows came back");
    assertEquals(OV_EARLY_CLOCK, instants[0], "raw instant of the earlier one");
    assertEquals(OV_LATE_CLOCK, instants[1], "raw instant of the later one");
    assertEquals(shown[0], shown[1], "an hour apart in absolute time, one identical LA clock");
    assertEquals(LocalDateTime.parse("2026-11-01T01:30"), shown[0], "that shared clock");
    assertEquals(OV_EARLY_CLOCK, ntz[0], "NTZ holds a clock, not two instants");
    assertEquals(OV_LATE_CLOCK, ntz[1], "so it stays where the absolute column collapsed");
  }

  // ---------------------------------------------------------------- A3: precision rules

  /**
   * What each type keeps of a fraction, measured end to end, plus the epoch-boundary rule.
   */
  @Test
  public void subSecondPrecisionRules() throws Exception {
    TimeZone previous = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone(UTC));
    try (Connection c = TestUtils.getConnectionWithTimezone(UTC)) {
      try (Statement st = c.createStatement()) {
        st.execute("set odps.sql.type.system.odps2=true;");
      }
      // 1. a fractional DATETIME literal is refused outright
      SQLException refused = assertThrows(SQLException.class, () -> {
        try (Statement st = c.createStatement()) {
          st.execute("set odps.sql.type.system.odps2=true;");
          st.execute("insert into " + TABLE + " values ('frac', date'2026-07-15',"
                     + " datetime'2026-07-15 12:34:56.999', timestamp'2026-07-15 12:34:56',"
                     + " timestamp_ntz'2026-07-15 12:34:56')");
        }
      });
      assertTrue(refused.getMessage().contains("invalid DATETIME format"), refused.getMessage());
      // ...while the same text is accepted as a TIMESTAMP literal

      // 2. milliseconds reach a DATETIME column only through the tunnel path
      Timestamp fraction = Timestamp.valueOf("2026-07-15 12:34:56.987654321");
      try (PreparedStatement ps = c.prepareStatement(
          "insert into " + WRITE_TABLE + " (k, dt, ts) values (?, ?, ?)")) {
        ps.setString(1, "millisViaTunnel");
        ps.setTimestamp(2, fraction);
        ps.setTimestamp(3, fraction);
        ps.executeUpdate();
      }
      try (Statement st = c.createStatement();
           ResultSet rs = st.executeQuery(
               "select dt, ts from " + WRITE_TABLE + " where k='millisViaTunnel';")) {
        assertTrue(rs.next());
        assertEquals(987000000, rs.getTimestamp(1).getNanos(),
                     "DATETIME is millisecond precision: .987654321 comes back .987, truncated");
        assertEquals(987654321, rs.getTimestamp(2).getNanos(), "TIMESTAMP keeps all nine");
      }
    } finally {
      TimeZone.setDefault(previous);
    }

    // 3. the epoch boundary: the fraction stays on its own side of the second
    under(UTC, UTC, rs -> {
      String key = rs.getString(1);
      if (!SEEDED.contains(key)) {
        return;
      }
      if (PRE_EPOCH.equals(key)) {
        assertEquals(-877L, rs.getTimestamp(4).getTime(),
                     "1969-12-31T23:59:59.123456789Z floors to -877 ms, not -876");
        assertEquals(123456789, rs.getTimestamp(4).getNanos(), "and keeps its own nanoseconds");
        assertEquals(-877L, rs.getTimestamp(5).getTime(), "same anchor for the NTZ copy");
        assertEquals(123456789, rs.getTimestamp(5).getNanos(), "same fraction for the NTZ copy");
        assertEquals(Date.valueOf("1969-12-31"), rs.getDate(5),
                     "the NTZ date stays on the far side of the epoch");
      } else if (FIRST_NANO.equals(key)) {
        assertEquals(0L, rs.getTimestamp(4).getTime(), "one nanosecond past the epoch is 0 ms");
        assertEquals(1, rs.getTimestamp(4).getNanos(), "and the single nanosecond survives");
        assertEquals(1, rs.getTimestamp(5).getNanos(), "same for the NTZ copy");
      }
    });
  }

  // ---------------------------------------------------------------- A2: write side

  /**
   * The tunnel write path is timezone-safe by construction: a bound {@code java.sql.Timestamp}
   * carries an instant and that instant has to land whatever the JVM default zone is.
   */
  @Test
  public void tunnelWriteKeepsTheBoundInstantInAnyJvmZone() throws Exception {
    Instant bound = LocalDateTime.parse("2030-02-03T04:05:06.789012345").atZone(zone(UTC))
        .toInstant();
    for (String jvm : new String[] {UTC, SHANGHAI, LOS_ANGELES}) {
      TimeZone previous = TimeZone.getDefault();
      TimeZone.setDefault(TimeZone.getTimeZone(jvm));
      try (Connection c = TestUtils.getConnectionWithTimezone(UTC);
           Statement st = c.createStatement()) {
        st.execute("set odps.sql.type.system.odps2=true;");
        try (PreparedStatement ps = c.prepareStatement(
            "insert into " + WRITE_TABLE + " (k, dt, ts) values (?, ?, ?)")) {
          ps.setString(1, "jvm:" + jvm);
          ps.setTimestamp(2, Timestamp.from(bound));
          ps.setTimestamp(3, Timestamp.from(bound));
          ps.executeUpdate();
        }
      } finally {
        TimeZone.setDefault(previous);
      }
    }
    final Instant[] read = new Instant[3];
    final int[] seen = {0};
    TimeZone previous = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone(SHANGHAI));
    try (Connection c = TestUtils.getConnectionWithTimezone(UTC);
         Statement st = c.createStatement()) {
      st.execute("set odps.sql.type.system.odps2=true;");
      try (ResultSet rs = st.executeQuery(
          "select k, ts from " + WRITE_TABLE + " where k like 'jvm:%';")) {
        while (rs.next()) {
          read[seen[0]++] = (Instant) rs.getObject(2);
        }
      }
    } finally {
      TimeZone.setDefault(previous);
    }
    assertEquals(3, seen[0], "one row per JVM timezone");
    for (int i = 0; i < seen[0]; i++) {
      assertEquals(bound, read[i], "row " + i + ": the JVM zone must not reach the data");
    }
  }

  /**
   * The Calendar overloads on the write side are not implemented. JDBC allows a driver to decline
   * them, so the contract is pinned rather than changed -- and it is why the read-side Calendar
   * behaviour above is the only place a caller can name a zone.
   */
  @Test
  public void calendarArgumentsOnTheWriteSideAreUnsupported() throws Exception {
    TimeZone previous = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone(UTC));
    try (Connection c = TestUtils.getConnectionWithTimezone(UTC);
         PreparedStatement ps = c.prepareStatement("insert into " + TABLE + " values (?,?,?,?,?)")) {
      Timestamp ts = Timestamp.valueOf("2030-02-03 04:05:06");
      assertThrows(SQLFeatureNotSupportedException.class,
                   () -> ps.setDate(2, new Date(0L), cal(UTC)));
      assertThrows(SQLFeatureNotSupportedException.class,
                   () -> ps.setTime(3, new Time(0L), cal(UTC)));
      assertThrows(SQLFeatureNotSupportedException.class,
                   () -> ps.setTimestamp(4, ts, cal(UTC)));
    } finally {
      TimeZone.setDefault(previous);
    }
  }

  /**
   * TIMESTAMP_NTZ can be read but not bound: the tunnel path has no transformer for the type, and
   * the SQL-literal path can only emit a {@code TIMESTAMP'} literal, which the server refuses to
   * store into a TIMESTAMP_NTZ column. A {@code LocalDateTime} cannot be bound either. Pinned with
   * the messages so the next reader does not spend an hour hunting for a binding that is not there.
   */
  @Test
  public void ntzCannotBeBoundThroughPreparedStatement() throws Exception {
    TimeZone previous = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone(UTC));
    try (Connection c = TestUtils.getConnectionWithTimezone(UTC)) {
      try (Statement st = c.createStatement()) {
        st.execute("set odps.sql.type.system.odps2=true;");
      }
      // tunnel path: the transformer table has no TIMESTAMP_NTZ entry at all
      try (PreparedStatement ps = c.prepareStatement(
          "insert into " + TABLE + " (k, tsntz) values (?, ?)")) {
        ps.setString(1, "ntz-tunnel");
        ps.setTimestamp(2, Timestamp.valueOf("2030-02-03 04:05:06"));
        SQLException tunnel = assertThrows(SQLException.class, ps::executeUpdate);
        assertTrue(tunnel.getMessage().contains("TIMESTAMP_NTZ"), tunnel.getMessage());
      }
      // and the lookup is per column, so even a NULL NTZ column inside the column list breaks it
      try (PreparedStatement ps = c.prepareStatement(
          "insert into " + TABLE + " (k, tsntz) values (?, ?)")) {
        ps.setString(1, "ntz-tunnel-null");
        ps.setNull(2, java.sql.Types.TIMESTAMP);
        SQLException nullColumn = assertThrows(SQLException.class, ps::executeUpdate);
        assertTrue(nullColumn.getMessage().contains("TIMESTAMP_NTZ"), nullColumn.getMessage());
      }
      // SQL-literal path: only a TIMESTAMP'...' literal can be emitted, which the server refuses
      try (PreparedStatement ps = c.prepareStatement(
          "insert into " + TABLE + " (k, tsntz) values (?, ?)")) {
        ps.setString(1, "ntz-literal");
        ps.setTimestamp(2, Timestamp.valueOf("2030-02-03 04:05:06"));
        SQLException literal = assertThrows(SQLException.class, ps::execute);
        assertTrue(literal.getMessage().contains("TIMESTAMP_NTZ"), literal.getMessage());
      }
      // a java.time clock -- the natural payload for a NTZ column -- is refused while binding
      try (PreparedStatement ps = c.prepareStatement(
          "insert into " + TABLE + " (k, tsntz) values (?, ?)")) {
        ps.setString(1, "ntz-clock");
        assertThrows(SQLException.class, () -> ps.setObject(2, SH_GAP_CLOCK));
      }
    } finally {
      TimeZone.setDefault(previous);
    }
  }
}
