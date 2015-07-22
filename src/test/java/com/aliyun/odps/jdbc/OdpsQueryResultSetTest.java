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
    Object o1 = rs.getObject(1);
    Object o2 = rs.getObject(2);
    assertEquals(1, ((Long) o1).longValue());
    assertEquals(1.5, ((Double) o2).doubleValue());

    rs.next();
    o1 = rs.getObject(1);
    o2 = rs.getObject(2);
    assertEquals(2, ((Long) o1).longValue());
    assertEquals(2.9, ((Double) o2).doubleValue());

    rs.close();
  }

  public void testGetInt() throws Exception {
    ResultSet rs = stmt.executeQuery("select 1 id from dual;");
    rs.next();
    int x = rs.getInt(1);
    assertEquals(1, x);

    // overflow
    rs.close();
    long l = (long) Integer.MAX_VALUE + 1;
    rs = stmt.executeQuery("select " + l + " id from dual;");
    rs.next();
    x = rs.getInt(1);
    assertEquals(Integer.MIN_VALUE, x);
  }

  public void testGetShort() throws Exception {
    ResultSet rs = stmt.executeQuery("select 1 id from dual;");
    rs.next();
    int x = rs.getShort(1);
    assertEquals(1, x);
  }

  public void testGetLong() throws Exception {
    ResultSet rs = stmt.executeQuery("select 1 id from dual;");
    rs.next();
    long x = rs.getLong(1);
    assertEquals(1, x);

    // upper value
    rs = stmt.executeQuery("select " + Long.MAX_VALUE + " id from dual;");
    rs.next();
    x = rs.getLong(1);
    assertEquals(Long.MAX_VALUE, x);
  }

  public void testGetDouble() throws Exception {
    ResultSet rs = stmt.executeQuery("select 0.5 weight from dual;");
    rs.next();
    double x = rs.getDouble(1);
    assertEquals(0.5, x);
  }

  public void testGetFloat() throws Exception {
    ResultSet rs = stmt.executeQuery("select 0.5 weight from dual;");
    rs.next();
    double x = rs.getFloat(1);
    assertEquals(0.5, x);
  }

  public void testGetBigDecimal() throws Exception {
    ResultSet rs = stmt.executeQuery("select 5555 weight from dual;");
    rs.next();
    BigDecimal x = rs.getBigDecimal(1);
    BigDecimal y = new BigDecimal(5555);
    assertEquals(y, x);
  }

  public void testGetDate() throws Exception {
    ResultSet rs = stmt.executeQuery(
        "select cast('" + odpsDatetimeNow + "' as datetime) day from dual;");

    rs.next();
    Date x = rs.getDate(1);
    Date y = new Date(unixTimeNow);
    assertEquals(y.toString(), x.toString());
  }

  public void testGetTime() throws Exception {
    ResultSet rs = stmt.executeQuery(
        "select cast('" + odpsDatetimeNow + "' as datetime) day from dual;");

    rs.next();
    Time x = rs.getTime(1);
    Time y = new Time(unixTimeNow);
    assertEquals(y.toString(), x.toString());
  }

  public void testGetTimeStamp() throws Exception {
    ResultSet rs = stmt.executeQuery(
        "select cast('"+ odpsDatetimeNow +"' as datetime) day from dual;");

    rs.next();
    Timestamp x = rs.getTimestamp(1);
    Timestamp y = new Timestamp(unixTimeNow);

    // Walk around the precision problem
    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    String ys = formatter.format(y);
    String xs = formatter.format(x);

    assertEquals(ys, xs);
  }

  public void testGetString() throws Exception {
    ResultSet rs = stmt.executeQuery(
        "select 'alibaba' name  from dual;");

    rs.next();
    String x = rs.getString(1);
    assertEquals("alibaba", x);
  }
}
