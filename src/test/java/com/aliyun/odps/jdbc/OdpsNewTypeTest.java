/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */

package com.aliyun.odps.jdbc;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Binary;
import com.aliyun.odps.data.Char;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.data.Varchar;
import com.aliyun.odps.jdbc.utils.TestUtils;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;

public class OdpsNewTypeTest {

  static Statement stmt;
  static ResultSet rs;
  static ResultSetMetaData rsmd;
  static String table = "jdbc_test_" + System.currentTimeMillis();
  static Odps odps = TestManager.getInstance().odps;
  static int v1;
  static long v2;
  static String v3;
  static BigDecimal v4;
  static byte v5;
  static short v6;
  static double v7;
  static float v8;
  static boolean v9;
  static Date v10;
  static java.util.Date v11;
  static Timestamp v12;
  static Varchar v13;
  static Char v14;
  static Binary v15;
  static List v16;
  static Map<Long, String> v17;
  static SimpleStruct v18;


  @BeforeClass
  public static void setUp() throws Exception {
    stmt = TestManager.getInstance().conn.createStatement();
    stmt.executeUpdate("drop table if exists " + table + ";");
    createTableWithAllNewTypes(odps, table);
    uploadData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (rs != null) {
      rs.close();
    }
    if (stmt != null) {
      stmt.close();
    }
    // odps.tables().delete(table, true);
  }

  @Test
  public void testQuery() throws SQLException {
    String sql = "select * from " + table;
    ResultSet rs = stmt.executeQuery(sql);
    while (rs.next()) {
      System.out.println(v1);
      System.out.println(rs.getString(1));
      Assert.assertEquals(v1, rs.getInt(1));
      System.out.println(v2);
      System.out.println(rs.getString(2));
      Assert.assertEquals(v2, rs.getLong(2));
      System.out.println(v3);
      System.out.println(rs.getString(3));
      Assert.assertEquals(v3, rs.getString(3));
      System.out.println(v4);
      System.out.println(rs.getString(4));
      Assert.assertEquals(v4, rs.getBigDecimal(4));
      System.out.println(v5);
      System.out.println(rs.getString(5));
      Assert.assertEquals(v5, rs.getByte(5));
      System.out.println(v6);
      System.out.println(rs.getString(6));
      Assert.assertEquals(v6, rs.getShort(6));
      System.out.println(v7);
      System.out.println(rs.getString(7));
      Assert.assertEquals(v7, rs.getDouble(7), 0.0000000001);
      System.out.println(v8);
      System.out.println(rs.getString(8));
      Assert.assertEquals(v8, rs.getFloat(8), 0.00001);
      System.out.println(v9);
      System.out.println(rs.getString(9));
      Assert.assertEquals(v9, rs.getBoolean(9));
      System.out.println(v10);
      System.out.println(rs.getString(10));
      Assert.assertEquals(v10.toString(), rs.getDate(10).toString());
      System.out.println(v11);
      System.out.println(rs.getString(11));
      Assert.assertEquals(v11.getTime(), rs.getTimestamp(11).getTime());
      System.out.println(v12);
      System.out.println(rs.getString(12));
      Assert.assertEquals(new Timestamp(v12.getTime()), rs.getTimestamp(12));
      System.out.println(v13);
      System.out.println(rs.getString(13));
      Assert.assertEquals(v13.toString(), rs.getString(13));
      Assert.assertEquals(v13, rs.getObject(13));
      System.out.println("v14:" + v14);
      System.out.println("v14 length:" + v14.length());
      System.out.println(rs.getString(14));
      System.out.println(rs.getString(14).length());
      Assert.assertNotEquals(v14.toString(), rs.getString(14));
      Assert.assertEquals(v14.toString().trim(), rs.getString(14).trim());
      Assert.assertEquals(2, rs.getString(14).length());
      System.out.println(v15);
      System.out.println(rs.getString(15));
      Assert.assertEquals(v15.toString(), rs.getString(15));
      Assert.assertEquals(v15, rs.getObject(15));
      System.out.println(v16);
      System.out.println(rs.getString(16));
      Assert.assertEquals(v16.toString(), rs.getString(16));
      Assert.assertEquals(v16, rs.getObject(16));
      System.out.println(v17);
      System.out.println(rs.getString(17));
      Assert.assertEquals(v17.toString(), rs.getString(17));
      Assert.assertEquals(v17, rs.getObject(17));
      System.out.println(v18.getFieldCount());
      System.out.println(((SimpleStruct) rs.getObject(18)).getFieldCount());
      Assert.assertEquals(v18.getFieldCount(), ((SimpleStruct) rs.getObject(18)).getFieldCount());
      List<Object> fields = ((SimpleStruct) rs.getObject(18)).getFieldValues();
      for (int i = 0; i < v18.getFieldValues().size(); i++) {
        System.out.println("index:" + i);
        System.out.println(v18.getFieldValues().get(i));
        System.out.println(fields.get(i));
        if (fields.get(i) instanceof Double) {
          Assert.assertEquals((Double) v18.getFieldValues().get(i), (Double) fields.get(i),
                              0.0000000001);
        } else if (fields.get(i) instanceof Float) {
          Assert.assertEquals((Float) v18.getFieldValues().get(i), (Float) fields.get(i), 0.00001);
        } else if (fields.get(i) instanceof Map) {
          Map m1 = (Map) v18.getFieldValues().get(i);
          Map m2 = new TreeMap((Map) fields.get(i));
          Assert.assertNotEquals(m1.toString(), m2.toString());
          for (Object k : m1.keySet()) {
            String v1 = m1.get(k).toString().trim();
            String v2 = m2.get(k).toString().trim();
            Assert.assertEquals(v1, v2);
          }
        } else {
          Assert.assertEquals(v18.getFieldValues().get(i), fields.get(i));
        }
      }
    }
  }

  private static void uploadData() throws TunnelException, IOException {
    TableTunnel.UploadSession up =
        TestManager.getInstance().tunnel.createUploadSession(odps.getDefaultProject(), table);
    RecordWriter writer = up.openRecordWriter(0);
    ArrayRecord r = new ArrayRecord(up.getSchema().getColumns().toArray(new Column[0]));

    v1 = TestUtils.randomInt();
    r.setInt(0, v1);
    v2 = TestUtils.randomLong();
    r.setBigint(1, v2);
    v3 = TestUtils.randomString();
    r.setString(2, v3);
    v4 = new BigDecimal("10.231");
    r.setDecimal(3, v4);
    v5 = TestUtils.randomByte();
    r.setTinyint(4, v5);
    v6 = TestUtils.randomShort();
    r.setSmallint(5, v6);
    v7 = TestUtils.randomDouble();
    r.setDouble(6, v7);
    v8 = TestUtils.randomFloat();
    r.setFloat(7, v8);
    v9 = TestUtils.randomBoolean();
    r.setBoolean(8, v9);
    v11 = TestUtils.randomDate();
    v10 = new Date(v11.getTime());
    r.setDate(9, v10);
    r.setDatetime(10, v11);
    v12 = new Timestamp(v11.getTime());
    r.setTimestamp(11, v12);
    v13 = new Varchar(TestUtils.randomString(1), 1);
    r.setVarchar(12, v13);
    v14 = new Char(TestUtils.randomString(1), 2);
    r.setChar(13, v14);
    v15 = new Binary(TestUtils.randomBytes());
    r.setBinary(14, v15);
    Integer[] ints = {TestUtils.randomInt(), TestUtils.randomInt(), TestUtils.randomInt()};
    v16 = Arrays.asList(ints);
    r.setArray(15, v16);
    v17 = new HashMap<Long, String>();
    v17.put(TestUtils.randomLong(), TestUtils.randomString());
    r.setMap(16, v17);
    List<Object> values = new ArrayList<Object>();
    values.add(TestUtils.randomString());
    values.add(TestUtils.randomInt());
    Map<Varchar, Char> parents = new TreeMap<Varchar, Char>();
    parents.put(new Varchar("papa"), new Char(TestUtils.randomString(10)));
    parents.put(new Varchar("mama"), new Char(TestUtils.randomString(10)));
    values.add(parents);
    values.add(TestUtils.randomFloat());
    String[] hobbies = {TestUtils.randomString(), TestUtils.randomString()};
    values.add(Arrays.asList(hobbies));
    v18 =
        new SimpleStruct((StructTypeInfo) up.getSchema().getColumn("col18").getTypeInfo(), values);
    r.setStruct(17, v18);
    writer.write(r);
    writer.close();
    Long[] blocks = up.getBlockList();
    Assert.assertEquals(1, blocks.length);
    up.commit(new Long[]{0L});
  }

  private static void createTableWithAllNewTypes(Odps odps, String tableName) throws OdpsException,
                                                                                     SQLException {
    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("col0", OdpsType.INT));
    schema.addColumn(new Column("col1", OdpsType.BIGINT));
    schema.addColumn(new Column("col2", OdpsType.STRING));
    schema.addColumn(new Column("col3", OdpsType.DECIMAL));
    schema.addColumn(new Column("col4", OdpsType.TINYINT));
    schema.addColumn(new Column("col5", OdpsType.SMALLINT));
    schema.addColumn(new Column("col6", OdpsType.DOUBLE));
    schema.addColumn(new Column("col7", OdpsType.FLOAT));
    schema.addColumn(new Column("col8", OdpsType.BOOLEAN));
    schema.addColumn(new Column("col9", OdpsType.DATE));
    schema.addColumn(new Column("col10", OdpsType.DATETIME));
    schema.addColumn(new Column("col11", OdpsType.TIMESTAMP));
    schema.addColumn(new Column("col13", TypeInfoFactory.getVarcharTypeInfo(2)));
    schema.addColumn(new Column("col14", TypeInfoFactory.getCharTypeInfo(2)));
    schema.addColumn(new Column("col15", OdpsType.BINARY));
    schema.addColumn(new Column("col16", TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.INT)));
    schema.addColumn(new Column("col17", TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.BIGINT,
                                                                        TypeInfoFactory.STRING)));
    String[] names = {"name", "age", "parents", "salary", "hobbies"};
    TypeInfo[] types =
        {
            TypeInfoFactory.STRING,
            TypeInfoFactory.INT,
            TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.getVarcharTypeInfo(20),
                                           TypeInfoFactory.getCharTypeInfo(20)),
            TypeInfoFactory.FLOAT,
            TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.STRING)};
    schema.addColumn(new Column("col18", TypeInfoFactory.getStructTypeInfo(Arrays.asList(names),
                                                                           Arrays.asList(types))));

    createTableWithHints(odps, tableName, schema);
  }

  private static void createTableWithHints(Odps odps, String tableName, TableSchema schema)
      throws OdpsException, SQLException {
    String sql = getSQLString(odps.getDefaultProject(), tableName, schema, null, false, null);
    System.out.println(sql);
    stmt.execute(sql);
  }

  private static String getSQLString(String projectName, String tableName, TableSchema schema,
                                     String comment, boolean ifNotExists, Long lifeCycle) {
    if (projectName == null || tableName == null || schema == null) {
      throw new IllegalArgumentException();
    }

    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE ");
    if (ifNotExists) {
      sb.append(" IF NOT EXISTS ");
    }

    sb.append(projectName).append(".`").append(tableName).append("` (");

    List<Column> columns = schema.getColumns();
    for (int i = 0; i < columns.size(); i++) {
      Column c = columns.get(i);
      sb.append("`").append(c.getName()).append("` ").append(c.getTypeInfo().getTypeName());
      if (c.getComment() != null) {
        sb.append(" COMMENT '").append(c.getComment()).append("'");
      }
      if (i + 1 < columns.size()) {
        sb.append(',');
      }
    }

    sb.append(')');

    if (comment != null) {
      sb.append(" COMMENT '" + comment + "' ");
    }

    List<Column> pcolumns = schema.getPartitionColumns();
    if (pcolumns.size() > 0) {
      sb.append(" PARTITIONED BY (");
      for (int i = 0; i < pcolumns.size(); i++) {
        Column c = pcolumns.get(i);
        sb.append(c.getName()).append(" ").append(c.getTypeInfo().getTypeName());
        if (c.getComment() != null) {
          sb.append(" COMMENT '").append(c.getComment()).append("'");
        }
        if (i + 1 < pcolumns.size()) {
          sb.append(',');
        }
      }
      sb.append(')');
    }

    if (lifeCycle != null) {
      sb.append(" LIFECYCLE ").append(lifeCycle);
    }

    sb.append(';');

    return sb.toString();
  }

}
