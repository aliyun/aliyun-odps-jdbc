package com.aliyun.odps.jdbc;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Date;
import java.text.Format;
import java.text.SimpleDateFormat;

import junit.framework.TestCase;

public class OdpsQueryResultSetTest extends TestCase {

  protected Statement stmt;
  protected long unixTimeNow;
  protected String odpsDatetimeNow;

  protected void setUp() throws Exception {
    stmt = OdpsConnectionFactory.getInstance().conn.createStatement();
    unixTimeNow = new java.util.Date().getTime();
    Format formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    odpsDatetimeNow = formatter.format(unixTimeNow);
  }

  protected void tearDown() throws Exception {
    stmt.close();
  }

  public void testGetObject() throws Exception {
    String sql = "select * from" + "(select 1 id, 1.5 weight from dual" + " union all"
                 + " select 2 id, 2.9 weight from dual) x;";
    ResultSet rs = stmt.executeQuery(sql);

    rs.next();
    assertEquals(1, ((Long) rs.getObject(1)).longValue());
    assertEquals(1, ((Long) rs.getObject("id")).longValue());

    assertEquals(1.5, ((Double) rs.getObject(2)).doubleValue());
    assertEquals(1.5, ((Double) rs.getObject("weight")).doubleValue());

    rs.next();
    assertEquals(2, ((Long) rs.getObject(1)).longValue());
    assertEquals(2, ((Long) rs.getObject("id")).longValue());

    assertEquals(2.9, ((Double) rs.getObject(2)).doubleValue());
    assertEquals(2.9, ((Double) rs.getObject("weight")).doubleValue());

    rs.close();
  }

  public void testGetByte() throws Exception {
    // cast from BIGINT
    ResultSet rs = stmt.executeQuery("select " + Long.MAX_VALUE + " id from dual;");
    rs.next();
    assertEquals((byte) Long.MAX_VALUE, rs.getByte(1));
    assertEquals((byte) Long.MAX_VALUE, rs.getByte("id"));
    rs.close();

  }

  public void testGetBoolean() throws Exception {
    // cast from BOOLEAN
    ResultSet rs = stmt.executeQuery("select " + Boolean.TRUE + " male from dual;");
    rs.next();
    assertEquals(true, rs.getBoolean(1));
    assertEquals(true, rs.getBoolean("male"));
    rs.close();

    // cast from STRING
    rs = stmt.executeQuery("select '0' namex, '42' namey from dual;");
    rs.next();
    assertEquals(false, rs.getBoolean(1));
    assertEquals(true, rs.getBoolean(2));
    rs.close();

    // cast from BIGINT
    rs = stmt.executeQuery("select 42 idx, 0 idy from dual;");
    rs.next();
    assertEquals(true, rs.getBoolean(1));
    assertEquals(false, rs.getBoolean(2));
    rs.close();
  }

  public void testGetInt() throws Exception {
    // cast from BIGINT
    ResultSet rs = stmt.executeQuery("select " + Long.MAX_VALUE + " id from dual;");
    rs.next();
    assertEquals((int) Long.MAX_VALUE, rs.getInt(1));
    assertEquals((int) Long.MAX_VALUE, rs.getInt("id"));
    rs.close();

    // cast from STRING
    rs = stmt.executeQuery("select '" + Long.MAX_VALUE + "' name from dual;");
    rs.next();
    assertEquals((int) Long.MAX_VALUE, rs.getInt(1));
    rs.close();
  }

  public void testGetShort() throws Exception {
    // cast from BIGINT
    ResultSet rs = stmt.executeQuery("select " + Long.MAX_VALUE + " id from dual;");
    rs.next();
    assertEquals((short) Long.MAX_VALUE, rs.getShort(1));
    assertEquals((short) Long.MAX_VALUE, rs.getShort("id"));
    rs.close();

    // cast from STRING
    rs = stmt.executeQuery("select '" + Long.MAX_VALUE + "' name from dual;");
    rs.next();
    assertEquals((short) Long.MAX_VALUE, rs.getShort(1));
    rs.close();
  }

  public void testGetLong() throws Exception {
    // cast from BIGINT
    ResultSet rs = stmt.executeQuery("select " + Long.MAX_VALUE + " id from dual;");
    rs.next();
    assertEquals(Long.MAX_VALUE, rs.getLong(1));
    assertEquals(Long.MAX_VALUE, rs.getLong("id"));
    rs.close();

    // cast from STRING
    rs = stmt.executeQuery("select '" + Long.MAX_VALUE + "' name from dual;");
    rs.next();
    assertEquals(Long.MAX_VALUE, rs.getLong(1));
    rs.close();
  }

  public void testGetDouble() throws Exception {
    // cast from DOUBLE
    ResultSet rs = stmt.executeQuery("select " + Double.MAX_VALUE + " weight from dual;");
    rs.next();
    assertEquals(Double.MAX_VALUE, rs.getDouble(1));
    assertEquals(Double.MAX_VALUE, rs.getDouble("weight"));
    rs.close();

    // cast from STRING
    rs = stmt.executeQuery("select '" + Double.MAX_VALUE + "' name from dual;");
    rs.next();
    assertEquals(Double.MAX_VALUE, rs.getDouble(1));
    rs.close();

  }

  public void testGetFloat() throws Exception {
    // cast from DOUBLE
    ResultSet rs = stmt.executeQuery("select " + Double.MAX_VALUE + " weight from dual;");
    rs.next();
    assertEquals((float) Double.MAX_VALUE, rs.getFloat(1));
    assertEquals((float) Double.MAX_VALUE, rs.getFloat("weight"));
    rs.close();

    // cast from STRING
    rs = stmt.executeQuery("select '" + Double.MAX_VALUE + "' name from dual;");
    rs.next();
    assertEquals((float) Double.MAX_VALUE, rs.getFloat(1));
    rs.close();
  }

  public void testGetBigDecimal() throws Exception {
    // cast from DECIMAL
    String decimalStr = "55.123456789012345";
    ResultSet rs =
        stmt.executeQuery("select cast('" + decimalStr + "' as decimal) weight from dual;");
    rs.next();
    BigDecimal expect = new BigDecimal(decimalStr);
    assertEquals(expect, rs.getBigDecimal(1));
    assertEquals(expect, rs.getBigDecimal("weight"));
    rs.close();

    // cast from STRING
    rs = stmt.executeQuery("select '" + decimalStr + "' name from dual;");
    rs.next();
    assertEquals(expect, rs.getBigDecimal(1));
    rs.close();
  }

  public void testGetDate() throws Exception {
    // cast from DATETIME
    ResultSet rs = stmt.executeQuery(
        "select cast('" + odpsDatetimeNow + "' as datetime) day from dual;");

    rs.next();
    Date expect = new Date(unixTimeNow);
    assertEquals(expect.toString(), rs.getDate(1).toString());
    assertEquals(expect.toString(), rs.getDate("day").toString());
    rs.close();

    // cast from STRING
    rs = stmt.executeQuery("select '" + odpsDatetimeNow + "' name from dual;");
    rs.next();
    assertEquals(expect.toString(), rs.getDate(1).toString());
    rs.close();
  }

  public void testGetTime() throws Exception {
    // cast from DATETIME
    ResultSet rs = stmt.executeQuery(
        "select cast('" + odpsDatetimeNow + "' as datetime) day from dual;");

    rs.next();
    Time expect = new Time(unixTimeNow);
    assertEquals(expect.toString(), rs.getTime(1).toString());
    assertEquals(expect.toString(), rs.getTime("day").toString());
    rs.close();

    // cast from STRING
    rs = stmt.executeQuery("select '" + odpsDatetimeNow + "' name from dual;");
    rs.next();
    assertEquals(expect.toString(), rs.getTime(1).toString());
    rs.close();
  }

  public void testGetTimeStamp() throws Exception {
    // cast from DATETIME
    ResultSet rs = stmt.executeQuery(
        "select cast('" + odpsDatetimeNow + "' as datetime) day from dual;");

    rs.next();
    Timestamp expect = new Timestamp(unixTimeNow);
    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    assertEquals(formatter.format(expect), formatter.format(rs.getTimestamp(1)));
    assertEquals(formatter.format(expect), formatter.format(rs.getTimestamp("day")));
    rs.close();

    // cast from STRING
    rs = stmt.executeQuery("select '" + odpsDatetimeNow + "' name from dual;");
    rs.next();
    assertEquals(formatter.format(expect), formatter.format(rs.getTimestamp(1)));
    rs.close();
  }

  public void testGetString() throws Exception {
    // cast from STRING
    ResultSet rs = stmt.executeQuery("select 'alibaba' name from dual;");
    rs.next();
    assertEquals("alibaba", rs.getString(1));
    assertEquals("alibaba", rs.getString("name"));
    rs.close();

    // cast from DOUBLE
    rs = stmt.executeQuery("select 0.5 weight from dual;");
    rs.next();
    assertEquals("0.5", rs.getString(1));
    rs.close();

    // cast from BIGINT
    rs = stmt.executeQuery("select 1 id from dual;");
    rs.next();
    assertEquals("1", rs.getString(1));
    rs.close();

    // cast from DATETIME
    rs = stmt.executeQuery(
        "select cast('" + odpsDatetimeNow + "' as datetime) day from dual;");
    rs.next();
    assertEquals(odpsDatetimeNow, rs.getString(1));
    rs.close();

    // cast from DECIMAL
    String decimalStr = "55.123456789012345";
    rs =
        stmt.executeQuery("select cast('" + decimalStr + "' as decimal) weight from dual;");
    rs.next();
    assertEquals(decimalStr, rs.getString(1));
    rs.close();


    // cast from BOOLEAN
    rs = stmt.executeQuery("select " + Boolean.TRUE + " male from dual;");
    rs.next();
    assertEquals(Boolean.TRUE.toString(), rs.getString(1));
    rs.close();
  }
}
