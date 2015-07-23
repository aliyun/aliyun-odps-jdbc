package com.aliyun.odps.jdbc;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Date;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Properties;

import junit.framework.TestCase;

public class OdpsQueryResultSetTest extends TestCase {

  protected Connection conn;
  protected Statement stmt;
  protected long unixTimeNow;
  protected String odpsDatetimeNow;

  protected void setUp() throws Exception {
    OdpsDriver driver = OdpsDriver.instance;

    Properties info = new Properties();
    info.put("access_id", BVTConf.getAccessId());
    info.put("access_key", BVTConf.getAccessKey());
    info.put("project_name", BVTConf.getProjectName());
    String url = BVTConf.getEndPoint();

    conn = driver.connect("jdbc:odps:" + url, info);
    stmt = conn.createStatement();

    unixTimeNow = new java.util.Date().getTime();
    Format formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    odpsDatetimeNow = formatter.format(unixTimeNow);
  }

  protected void tearDown() throws Exception {
    stmt.close();
    conn.close();
  }

  public void testGetObject() throws Exception {
    String sql = "select * from" + "(select 1 id, 1.5 weight from dual" + " union all"
                 + " select 2 id, 2.9 weight from dual) x;";
    ResultSet rs = stmt.executeQuery(sql);

    rs.next();
    assertEquals(1, ((Long) rs.getObject(1)).longValue());
    assertEquals(1.5, ((Double) rs.getObject(2)).doubleValue());

    rs.next();
    assertEquals(2, ((Long) rs.getObject(1)).longValue());
    assertEquals(2.9, ((Double) rs.getObject(2)).doubleValue());

    rs.close();
  }

  public void testGetInt() throws Exception {
    ResultSet rs = stmt.executeQuery("select 1 id from dual;");
    rs.next();
    assertEquals(1, rs.getInt(1));
    assertEquals("1", rs.getString(1));  // cast to string
    assertEquals(1, rs.getLong(1));       // cast to long

    // cast from LONG
    rs.close();
    rs = stmt.executeQuery("select " + Long.MAX_VALUE + " id from dual;");
    rs.next();
    assertEquals((int) Long.MAX_VALUE, rs.getInt(1));
  }

  public void testGetShort() throws Exception {
    ResultSet rs = stmt.executeQuery("select 1 id from dual;");
    rs.next();
    assertEquals(1, rs.getShort(1));

    // cast from LONG
    rs.close();
    rs = stmt.executeQuery("select " + Long.MAX_VALUE + " id from dual;");
    rs.next();
    assertEquals((short) Long.MAX_VALUE, rs.getShort(1));
  }

  public void testGetLong() throws Exception {
    ResultSet rs = stmt.executeQuery("select " + Long.MAX_VALUE + " id from dual;");
    rs.next();
    assertEquals(Long.MAX_VALUE, rs.getLong(1));
  }

  public void testGetDouble() throws Exception {
    ResultSet rs = stmt.executeQuery("select 0.5 weight from dual;");
    rs.next();
    assertEquals(0.5, rs.getDouble(1));
  }

  public void testGetFloat() throws Exception {
    ResultSet rs = stmt.executeQuery("select 0.5 weight from dual;");
    rs.next();
    assertEquals((float) 0.5, rs.getFloat(1));

    // cast from DOUBLE
    rs.close();
    rs = stmt.executeQuery("select " + Double.MAX_VALUE + " weight from dual;");
    rs.next();
    assertEquals((float) Double.MAX_VALUE, rs.getFloat(1));
  }

  public void testGetBigDecimal() throws Exception {
    String decimalStr = "55.123456789012345";
    ResultSet rs =
        stmt.executeQuery("select cast('" + decimalStr + "' as decimal) weight from dual;");
    rs.next();
    BigDecimal real = rs.getBigDecimal(1);
    BigDecimal expect = new BigDecimal(decimalStr);
    assertEquals(expect, real);

    // cast from STRING
    rs.close();
    rs = stmt.executeQuery("select '" + decimalStr + "' name from dual;");
    rs.next();
    assertEquals(expect, rs.getBigDecimal(1));
  }

  public void testGetDate() throws Exception {
    ResultSet rs = stmt.executeQuery(
        "select cast('" + odpsDatetimeNow + "' as datetime) day from dual;");

    rs.next();
    Date real = rs.getDate(1);
    Date expect = new Date(unixTimeNow);
    assertEquals(expect.toString(), real.toString());

    // cast from STRING
    rs.close();
    rs = stmt.executeQuery("select '" + odpsDatetimeNow + "' name from dual;");
    rs.next();
    assertEquals(expect.toString(), rs.getDate(1).toString());
  }

  public void testGetTime() throws Exception {
    ResultSet rs = stmt.executeQuery(
        "select cast('" + odpsDatetimeNow + "' as datetime) day from dual;");

    rs.next();
    Time real = rs.getTime(1);
    Time expect = new Time(unixTimeNow);
    assertEquals(expect.toString(), real.toString());
  }

  public void testGetTimeStamp() throws Exception {
    ResultSet rs = stmt.executeQuery(
        "select cast('" + odpsDatetimeNow + "' as datetime) day from dual;");

    rs.next();
    Timestamp real = rs.getTimestamp(1);
    Timestamp expect = new Timestamp(unixTimeNow);

    // Walk around the precision problem
    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    assertEquals(formatter.format(expect), formatter.format(real));
  }

  public void testGetString() throws Exception {
    ResultSet rs = stmt.executeQuery("select 'alibaba' name from dual;");
    rs.next();
    assertEquals("alibaba", rs.getString(1));

    // cast from DOUBLE
    rs.close();
    rs = stmt.executeQuery("select 0.5 weight from dual;");
    rs.next();
    assertEquals("0.5", rs.getString(1));

    // cast from BIGINT
    rs.close();
    rs = stmt.executeQuery("select 1 id from dual;");
    rs.next();
    assertEquals("1", rs.getString(1));
  }
}
