package com.aliyun.odps.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.data.Varchar;
import com.aliyun.odps.jdbc.utils.JdbcColumn;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.UploadSession;
import com.aliyun.odps.type.TypeInfoFactory;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This test will create a table with all the data type of ODPS 2.0 and create
 */
public class ImplicitTypeConversionTest {
  private static final String TINYINT_COL = "t_tinyint";
  private static final String SMALLINT_COL = "t_smallint";
  private static final String INT_COL = "t_int";
  private static final String BIGINT_COL = "t_bigint";
  private static final String FLOAT_COL = "t_float";
  private static final String DOUBLT_COL = "t_double";
  private static final String DECIMAL_COL = "t_decimal";
  private static final String VARCHAR_COL = "t_varchar";
  private static final String STRING_COL_1 = "t_string_1";
  private static final String STRING_COL_2 = "t_string_2";
  private static final String DATETIME_COL = "t_datetime";
  private static final String TIMESTAMP_COL = "t_timestamp";
  private static final String BOOLEAN_COL = "t_boolean";

  private static final String TINYINT_VAL = "1";
  private static final String SMALLINT_VAL = "2";
  private static final String INT_VAL = "3";
  private static final String BIGINT_VAL = "4";
  private static final String FLOAT_VAL = "4.5";
  private static final String DOUBLE_VAL = "4.56";
  private static final String DECIMAL_VAL = "4.567";
  private static final String VARCHAR_VAL = "4.5678";
  private static final String STRING_VAL_1 = "5";
  private static final String STRING_VAL_2 = "2019-05-23 00:00:00";
  private static Date DATETIME_VAL;
  private static Timestamp TIMESTAMP_VAL;
  private static final boolean BOOLEAN_VAL = true;

  private static ResultSet rs;
  // The charset in configuration must be 'UTF-8', which is the default value, or the test may fail
  private static String charset = "UTF-8";
  private static SimpleDateFormat dateFormat =
      new SimpleDateFormat(JdbcColumn.ODPS_DATETIME_FORMAT);


  @BeforeClass
  public static void setUp() throws OdpsException, IOException, ParseException, SQLException {
    TestManager tm = TestManager.getInstance();
    tm.odps.tables().delete("implicit_type_conversion_test", true);
    TableSchema schema = new TableSchema();
    schema.addColumn(
        new Column(TINYINT_COL, TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.TINYINT)));
    schema.addColumn(
        new Column(SMALLINT_COL, TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.SMALLINT)));
    schema.addColumn(
        new Column(INT_COL, TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.INT)));
    schema.addColumn(
        new Column(BIGINT_COL, TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.BIGINT)));
    schema.addColumn(
        new Column(FLOAT_COL, TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.FLOAT)));
    schema.addColumn(
        new Column(DOUBLT_COL, TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.DOUBLE)));
    schema.addColumn(
        new Column(DECIMAL_COL, TypeInfoFactory.getDecimalTypeInfo(54,18)));
    schema.addColumn(
        new Column(VARCHAR_COL, TypeInfoFactory.getVarcharTypeInfo(200)));
    schema.addColumn(
        new Column(STRING_COL_1, TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.STRING)));
    schema.addColumn(
        new Column(STRING_COL_2, TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.STRING)));
    schema.addColumn(
        new Column(DATETIME_COL, TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.DATETIME)));
    schema.addColumn(
        new Column(TIMESTAMP_COL, TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.TIMESTAMP)));
    schema.addColumn(
        new Column(BOOLEAN_COL, TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.BOOLEAN)));

    Map<String, String> hints = new HashMap<String, String>();
    hints.put("odps.sql.type.system.odps2", "true");
    tm.odps.tables().create(
        tm.odps.getDefaultProject(),
        "implicit_type_conversion_test",
        schema,
        null,
        true,
        null,hints, null);

    TableTunnel tunnel = new TableTunnel(tm.odps);
    UploadSession uploadSession = tunnel.createUploadSession(
        tm.odps.getDefaultProject(), "implicit_type_conversion_test");
    RecordWriter writer = uploadSession.openRecordWriter(0);
    Record r = uploadSession.newRecord();
    r.set(TINYINT_COL, Byte.parseByte(TINYINT_VAL));
    r.set(SMALLINT_COL, Short.parseShort(SMALLINT_VAL));
    r.set(INT_COL, Integer.parseInt(INT_VAL));
    r.set(BIGINT_COL, Long.parseLong(BIGINT_VAL));
    r.set(FLOAT_COL, Float.parseFloat(FLOAT_VAL));
    r.set(DOUBLT_COL, Double.parseDouble(DOUBLE_VAL));
    r.set(DECIMAL_COL, new BigDecimal(DECIMAL_VAL));
    r.set(VARCHAR_COL, new Varchar(VARCHAR_VAL));
    r.set(STRING_COL_1, STRING_VAL_1);
    r.set(STRING_COL_2, STRING_VAL_2);
    DATETIME_VAL = dateFormat.parse("2019-05-23 00:00:00");
    r.set(DATETIME_COL, DATETIME_VAL);
    TIMESTAMP_VAL = Timestamp.valueOf("2019-05-23 00:00:00.123456789");

    r.set(TIMESTAMP_COL, TIMESTAMP_VAL);
    r.set(BOOLEAN_COL, BOOLEAN_VAL);
    writer.write(r);
    writer.close();
    uploadSession.commit();

    String sql = "select * from implicit_type_conversion_test";
    Statement stmt = tm.conn.createStatement();
    stmt.execute(sql);
    rs = stmt.getResultSet();
    rs.next();
  }

  @AfterClass
  public static void tearDown() throws OdpsException, SQLException {
    rs.close();
    TestManager tm = TestManager.getInstance();
    tm.odps.tables().delete("implicit_type_conversion_test", true);
  }

  /**
   * Test transformation from Byte object (from ODPS SDK) to JDBC types
   * @throws SQLException
   */
  @Test
  public void testGetTinyInt() throws SQLException {
    Byte expected = Byte.parseByte(TINYINT_VAL);
    byte byteRes = rs.getByte(TINYINT_COL);
    assertEquals(expected.byteValue(), byteRes);

    // Valid implicit conversions
    short shortRes = rs.getShort(TINYINT_COL);
    assertEquals(expected.shortValue(), shortRes);
    int intRes = rs.getInt(TINYINT_COL);
    assertEquals(expected.intValue(), intRes);
    long longRes = rs.getLong(TINYINT_COL);
    assertEquals(expected.longValue(), longRes);
    float floatRes = rs.getFloat(TINYINT_COL);
    assertEquals(expected.floatValue(), floatRes, 0.0001);
    double doubleRes = rs.getDouble(TINYINT_COL);
    assertEquals(expected.doubleValue(), doubleRes, 0.0001);
    String stringRes = rs.getString(TINYINT_COL);
    assertEquals(expected.toString(), stringRes);
    byte[] byteArrayRes = rs.getBytes(TINYINT_COL);
    assertEquals(new String(expected.toString().getBytes()), new String(byteArrayRes));
    boolean booleanRes = rs.getBoolean(TINYINT_COL);
    assertEquals(BOOLEAN_VAL, booleanRes);

    // Invalid implicit conversions
    try {
      rs.getBigDecimal(TINYINT_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getDate(TINYINT_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getTime(TINYINT_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getTimestamp(TINYINT_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
  }

  @Test
  public void testGetSmallInt() throws SQLException {
    Short expected = Short.parseShort(SMALLINT_VAL);
    short shortRes = rs.getShort(SMALLINT_COL);
    assertEquals(expected.shortValue(), shortRes);

    // Valid implicit conversions
    byte byteRes = rs.getByte(SMALLINT_COL);
    assertEquals(expected.byteValue(), byteRes);
    int intRes = rs.getInt(SMALLINT_COL);
    assertEquals(expected.intValue(), intRes);
    long longRes = rs.getLong(SMALLINT_COL);
    assertEquals(expected.longValue(), longRes);
    float floatRes = rs.getFloat(SMALLINT_COL);
    assertEquals(expected.floatValue(), floatRes, 0.0001);
    double doubleRes = rs.getDouble(SMALLINT_COL);
    assertEquals(expected.doubleValue(), doubleRes, 0.0001);
    String stringRes = rs.getString(SMALLINT_COL);
    assertEquals(expected.toString(), stringRes);
    byte[] byteArrayRes = rs.getBytes(SMALLINT_COL);
    assertEquals(new String(expected.toString().getBytes()), new String(byteArrayRes));
    boolean booleanRes = rs.getBoolean(SMALLINT_COL);
    assertEquals(BOOLEAN_VAL, booleanRes);

    // Invalid implicit conversions
    try {
      rs.getBigDecimal(SMALLINT_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getDate(SMALLINT_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getTime(SMALLINT_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getTimestamp(SMALLINT_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
  }

  @Test
  public void testGetInt() throws SQLException {
    Integer expected = Integer.parseInt(INT_VAL);
    int intRes = rs.getInt(INT_COL);
    assertEquals(expected.intValue(), intRes);

    // Valid implicit conversions
    byte byteRes = rs.getByte(INT_COL);
    assertEquals(expected.byteValue(), byteRes);
    short shortRes = rs.getShort(INT_COL);
    assertEquals(expected.shortValue(), shortRes);
    long longRes = rs.getLong(INT_COL);
    assertEquals(expected.longValue(), longRes);
    float floatRes = rs.getFloat(INT_COL);
    assertEquals(expected.floatValue(), floatRes, 0.0001);
    double doubleRes = rs.getDouble(INT_COL);
    assertEquals(expected.doubleValue(), doubleRes, 0.0001);
    String stringRes = rs.getString(INT_COL);
    assertEquals(expected.toString(), stringRes);
    byte[] byteArrayRes = rs.getBytes(INT_COL);
    assertEquals(new String(expected.toString().getBytes()), new String(byteArrayRes));
    boolean booleanRes = rs.getBoolean(INT_COL);
    assertEquals(BOOLEAN_VAL, booleanRes);

    // Invalid implicit conversions
    try {
      rs.getBigDecimal(INT_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getDate(INT_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getTime(INT_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getTimestamp(INT_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
  }

  @Test
  public void testGetBigInt() throws SQLException {
    Long expected = Long.parseLong(BIGINT_VAL);
    long longRes = rs.getLong(BIGINT_COL);
    assertEquals(expected.longValue(), longRes);

    // Valid implicit conversions
    byte byteRes = rs.getByte(BIGINT_COL);
    assertEquals(expected.byteValue(), byteRes);
    short shortRes = rs.getShort(BIGINT_COL);
    assertEquals(expected.shortValue(), shortRes);
    int intRes = rs.getInt(BIGINT_COL);
    assertEquals(expected.intValue(), intRes);
    float floatRes = rs.getFloat(BIGINT_COL);
    assertEquals(expected.floatValue(), floatRes, 0.0001);
    double doubleRes = rs.getDouble(BIGINT_COL);
    assertEquals(expected.doubleValue(), doubleRes, 0.0001);
    String stringRes = rs.getString(BIGINT_COL);
    assertEquals(expected.toString(), stringRes);
    byte[] byteArrayRes = rs.getBytes(BIGINT_COL);
    assertEquals(new String(expected.toString().getBytes()), new String(byteArrayRes));
    boolean booleanRes = rs.getBoolean(BIGINT_COL);
    assertEquals(BOOLEAN_VAL, booleanRes);

    // Invalid implicit conversions
    try {
      rs.getBigDecimal(BIGINT_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getDate(BIGINT_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getTime(BIGINT_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getTimestamp(BIGINT_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
  }

  @Test
  public void testGetFloat() throws SQLException {
    Float expected = Float.parseFloat(FLOAT_VAL);
    float floatRes = rs.getFloat(FLOAT_COL);
    assertEquals(expected, floatRes, 0.0001);

    // Valid implicit conversions
    byte byteRes = rs.getByte(FLOAT_COL);
    assertEquals(expected.byteValue(), byteRes);
    short shortRes = rs.getShort(FLOAT_COL);
    assertEquals(expected.shortValue(), shortRes);
    int intRes = rs.getInt(FLOAT_COL);
    assertEquals(expected.intValue(), intRes);
    long longRes = rs.getLong(FLOAT_COL);
    assertEquals(expected.longValue(), longRes);
    double doubleRes = rs.getDouble(FLOAT_COL);
    assertEquals(expected.doubleValue(), doubleRes, 0.0001);
    String stringRes = rs.getString(FLOAT_COL);
    assertEquals(expected.toString(), stringRes);
    byte[] byteArrayRes = rs.getBytes(FLOAT_COL);
    assertEquals(new String(expected.toString().getBytes()), new String(byteArrayRes));
    boolean booleanRes = rs.getBoolean(FLOAT_COL);
    assertEquals(BOOLEAN_VAL, booleanRes);

    // Invalid implicit conversions
    try {
      rs.getBigDecimal(FLOAT_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getDate(FLOAT_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getTime(FLOAT_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getTimestamp(FLOAT_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
  }

  @Test
  public void testGetDouble() throws SQLException {
    Double expected = Double.parseDouble(DOUBLE_VAL);
    double doubleRes = rs.getDouble(DOUBLT_COL);
    assertEquals(expected, doubleRes, 0.0001);

    // Valid implicit conversions
    byte byteRes = rs.getByte(DOUBLT_COL);
    assertEquals(expected.byteValue(), byteRes);
    short shortRes = rs.getShort(DOUBLT_COL);
    assertEquals(expected.shortValue(), shortRes);
    int intRes = rs.getInt(DOUBLT_COL);
    assertEquals(expected.intValue(), intRes);
    long longRes = rs.getLong(DOUBLT_COL);
    assertEquals(expected.longValue(), longRes);
    float floatRes = rs.getFloat(DOUBLT_COL);
    assertEquals(expected.floatValue(), floatRes, 0.0001);
    String stringRes = rs.getString(DOUBLT_COL);
    assertEquals(expected.toString(), stringRes);
    byte[] byteArrayRes = rs.getBytes(DOUBLT_COL);
    assertEquals(new String(expected.toString().getBytes()), new String(byteArrayRes));
    boolean booleanRes = rs.getBoolean(DOUBLT_COL);
    assertEquals(BOOLEAN_VAL, booleanRes);

    // Invalid implicit conversions
    try {
      rs.getBigDecimal(DOUBLT_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getDate(DOUBLT_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getTime(DOUBLT_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getTimestamp(DOUBLT_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
  }

  @Test
  public void testGetDecimal() throws SQLException {
    BigDecimal expected = new BigDecimal(DECIMAL_VAL);
    BigDecimal bigDecimalRes = rs.getBigDecimal(DECIMAL_COL);
    assertEquals(expected, bigDecimalRes);

    // Valid implicit conversions
    byte byteRes = rs.getByte(DECIMAL_COL);
    assertEquals(expected.byteValue(), byteRes);
    short shortRes = rs.getShort(DECIMAL_COL);
    assertEquals(expected.shortValue(), shortRes);
    int intRes = rs.getInt(DECIMAL_COL);
    assertEquals(expected.intValue(), intRes);
    long longRes = rs.getLong(DECIMAL_COL);
    assertEquals(expected.longValue(), longRes);
    float floatRes = rs.getFloat(DECIMAL_COL);
    assertEquals(expected.floatValue(), floatRes, 0.0001);
    double doubleRes = rs.getDouble(DECIMAL_COL);
    assertEquals(expected.doubleValue(), doubleRes, 0.0001);
    String stringRes = rs.getString(DECIMAL_COL);
    assertEquals(expected.toString(), stringRes);
    byte[] byteArrayRes = rs.getBytes(DECIMAL_COL);
    assertEquals(new String(expected.toString().getBytes()), new String(byteArrayRes));
    boolean booleanRes = rs.getBoolean(DECIMAL_COL);
    assertEquals(BOOLEAN_VAL, booleanRes);

    // Invalid implicit conversions
    try {
      rs.getDate(DECIMAL_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getTime(DECIMAL_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getTimestamp(DECIMAL_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
  }

  @Test
  public void testGetVarchar() throws SQLException, UnsupportedEncodingException {
    Varchar expected = new Varchar(VARCHAR_VAL);

    // Valid implicit conversions
    String stringRes = rs.getString(VARCHAR_COL);
    assertEquals(expected.toString(), stringRes);
    byte[] byteArrayRes = rs.getBytes(VARCHAR_COL);
    assertEquals(new String(expected.toString().getBytes(), charset), new String(byteArrayRes));

    // Invalid implicit conversions
    try {
      rs.getByte(VARCHAR_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getShort(VARCHAR_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getInt(VARCHAR_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getLong(VARCHAR_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getFloat(VARCHAR_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getDouble(VARCHAR_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getBigDecimal(VARCHAR_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getDate(DECIMAL_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getTime(DECIMAL_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getTimestamp(DECIMAL_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getBoolean(VARCHAR_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
  }

  @Test
  public void testGetString() throws SQLException, ParseException {
    String expected1 = STRING_VAL_1;
    String expected2 = STRING_VAL_2;
    String stringRes = rs.getString(STRING_COL_1);
    assertEquals(expected1, stringRes);

    // Valid implicit conversions
    short shortRes = rs.getShort(STRING_COL_1);
    assertEquals(Short.parseShort(expected1), shortRes);
    int intRes = rs.getInt(STRING_COL_1);
    assertEquals(Integer.parseInt(expected1), intRes);
    long longRes = rs.getLong(STRING_COL_1);
    assertEquals(Long.parseLong(expected1), longRes);
    float floatRes = rs.getFloat(STRING_COL_1);
    assertEquals(Float.parseFloat(expected1), floatRes, 0.0001);
    double doubleRes = rs.getDouble(STRING_COL_1);
    assertEquals(Double.parseDouble(expected1), doubleRes, 0.0001);
    byte[] byteArrayRes = rs.getBytes(STRING_COL_1);
    assertEquals(expected1, new String(byteArrayRes));
    Date date = dateFormat.parse(expected2);
    java.sql.Date dateRes = rs.getDate(STRING_COL_2);
    assertEquals(date, dateRes);
    // Because of JdbcColumn.ODPS_DATETIME_FORMAT, the millisecond part will be removed
    java.sql.Time timeRes = rs.getTime(STRING_COL_2);
    assertEquals(date.getTime(), timeRes.getTime());
    java.sql.Timestamp timestampRes = rs.getTimestamp(STRING_COL_2);
    assertEquals(Timestamp.valueOf(expected2), timestampRes);
    boolean booleanRes = rs.getBoolean(STRING_COL_1);
    assertEquals(BOOLEAN_VAL, booleanRes);

    // Invalid implicit conversions
    try {
      rs.getByte(STRING_COL_1);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }

    try {
      rs.getBigDecimal(STRING_COL_1);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
  }

  @Test
  public void testGetDatetime() throws SQLException, UnsupportedEncodingException, ParseException {
    java.sql.Date expected = new java.sql.Date(DATETIME_VAL.getTime());

    // Valid implicit conversions
    String stringRes = rs.getString(DATETIME_COL);
    assertEquals(dateFormat.format(expected), stringRes);
    byte[] byteArrayRes = rs.getBytes(DATETIME_COL);
    assertEquals(dateFormat.format(expected), new String(byteArrayRes, charset));
    java.sql.Date dateRes = rs.getDate(DATETIME_COL);
    assertEquals(expected, dateRes);
    java.sql.Time timeRes = rs.getTime(DATETIME_COL);
    assertEquals(expected.getTime(), timeRes.getTime());
    Timestamp timestampRes = rs.getTimestamp(DATETIME_COL);
    assertEquals(expected.getTime(), timestampRes.getTime());

    // Invalid implicit conversions
    try {
      rs.getByte(DATETIME_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getShort(DATETIME_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getInt(DATETIME_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getLong(DATETIME_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getFloat(DATETIME_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getDouble(DATETIME_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getBigDecimal(DATETIME_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getBoolean(DATETIME_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
  }

  @Test
  public void testGetTimestamp() throws SQLException, UnsupportedEncodingException, ParseException {
    Timestamp expected = TIMESTAMP_VAL;
    Timestamp timestampRes = rs.getTimestamp(TIMESTAMP_COL);
    assertEquals(expected, timestampRes);

    // Valid implicit conversions
    String stringRes = rs.getString(TIMESTAMP_COL);
    assertEquals(expected, Timestamp.valueOf(stringRes));
    byte[] byteArrayRes = rs.getBytes(TIMESTAMP_COL);
    assertEquals(expected, Timestamp.valueOf(new String(byteArrayRes, charset)));
    java.sql.Date dateRes = rs.getDate(TIMESTAMP_COL);
    assertEquals(expected.getTime(), dateRes.getTime());
    java.sql.Time timeRes = rs.getTime(TIMESTAMP_COL);
    assertEquals(expected.getTime(), timeRes.getTime());

    // Invalid implicit conversions
    try {
      rs.getByte(TIMESTAMP_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getShort(TIMESTAMP_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getInt(TIMESTAMP_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getLong(TIMESTAMP_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getFloat(TIMESTAMP_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getDouble(TIMESTAMP_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getBigDecimal(TIMESTAMP_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getBoolean(TIMESTAMP_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
  }

  @Test
  public void testGetBoolean() throws SQLException, UnsupportedEncodingException {
    boolean expected = BOOLEAN_VAL;
    boolean booleanRes = rs.getBoolean(BOOLEAN_COL);
    assertEquals(expected, booleanRes);

    // Valid implicit conversions
    String stringRes = rs.getString(BOOLEAN_COL);
    assertEquals(expected, Boolean.valueOf(stringRes));
    byte[] byteArrayRes = rs.getBytes(BOOLEAN_COL);
    assertEquals(expected, Boolean.valueOf(new String(byteArrayRes, charset)));

    // Invalid implicit conversions
    try {
      rs.getByte(BOOLEAN_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getShort(BOOLEAN_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getInt(BOOLEAN_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getLong(BOOLEAN_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getFloat(BOOLEAN_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getDouble(BOOLEAN_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getBigDecimal(BOOLEAN_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getDate(BOOLEAN_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getTime(BOOLEAN_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
    try {
      rs.getTimestamp(BOOLEAN_COL);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot transform"));
    }
  }
}
