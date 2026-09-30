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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.Table;
import com.aliyun.odps.jdbc.utils.ServerCapabilities;
import com.aliyun.odps.jdbc.utils.TestUtils;

/**
 * The metadata contract of a two-tier (non namespace-schema) project, read against the service.
 *
 * <p>Four things a consumer depends on and that a unit test cannot see:
 * <ul>
 *   <li>{@code getColumns()} reports the same identity -- {@code TABLE_CAT}, {@code TABLE_SCHEM},
 *       {@code TABLE_NAME} -- as the {@code getTables()} row of the same table, so the two result
 *       sets can be joined;</li>
 *   <li>{@code COLUMN_SIZE} / {@code DECIMAL_DIGITS} carry the declared precision and scale of the
 *       column, i.e. the numbers {@code ResultSetMetaData.getPrecision()/getScale()} report for a
 *       value read out of that column;</li>
 *   <li>a search criterion filters: {@code columnNamePattern} narrows the rows, a
 *       {@code schemaPattern} that matches nothing returns nothing, and {@code getCatalogs()} and
 *       {@code getSchemas()} agree;</li>
 *   <li>an object that is not there -- project, schema, table or column name -- is an empty result
 *       set, not a failed call.</li>
 * </ul>
 *
 * <p>Partition columns stay in {@code getColumns()} with ordinals continuing after the data
 * columns, and a table whose comment is not ASCII comes back byte for byte.
 *
 * <p>Every listing passes a name pattern the service can filter on: an unrestricted
 * {@code getTables()} enumerates the whole project, which on a shared test project costs minutes.
 */
@DisabledIf(value = "com.aliyun.odps.jdbc.MetadataColumnsContractTest#threeTierProject",
    disabledReason = "this class fixes the two-tier shape; the namespace-schema shape is "
        + "MetadataNamespaceContractTest")
public class MetadataColumnsContractTest {

  private static final String PREFIX = "jdbc_md_cols_";
  private static final String T_ALL = PREFIX + "all";
  private static final String T_CN = PREFIX + "cn";
  private static final String V_CN = PREFIX + "view";
  private static final String MISSING = PREFIX + "absent";

  private static final String ALL_TYPES =
      "id bigint, dec_col decimal(10,2), ch char(5), vc varchar(20), s string, dbl double, "
      + "f float, i int, ti tinyint, si smallint, ts timestamp, dt datetime, d date, "
      + "bl boolean, bin binary, arr array<bigint>, mp map<string,bigint>, "
      + "st2 struct<a:bigint,b:string>";

  private static Connection conn;
  private static DatabaseMetaData dmd;
  private static Odps odps;
  private static String project;

  static boolean threeTierProject() {
    try {
      return ServerCapabilities.namespaceSchemaEnabled(TestUtils.getOdps());
    } catch (Throwable t) {
      return true; // no test identity: skip loudly rather than fail on a connection we cannot make
    }
  }

  @BeforeAll
  public static void setUp() throws Exception {
    conn = TestUtils.getConnection();
    dmd = conn.getMetaData();
    odps = TestUtils.getOdps();
    project = odps.getDefaultProject();
    try (Statement st = conn.createStatement()) {
      st.execute("set odps.sql.type.system.odps2=true;");
      st.execute("set odps.sql.decimal.odps2=true;");
      st.execute("drop table if exists " + T_ALL + ";");
      st.execute("create table " + T_ALL + " (" + ALL_TYPES + ") "
          + "comment 'md contract all types' partitioned by (ds string, pt bigint);");
      st.execute("drop view if exists " + V_CN + ";");
      st.execute("drop table if exists " + T_CN + ";");
      st.execute("create table " + T_CN + " (id bigint, name string) "
          + "comment '中文表注释-特殊字符 <>&';");
      st.execute("create view " + V_CN + " as select id, name from " + T_CN + ";");
    }
  }

  @AfterAll
  public static void tearDown() {
    if (conn == null) {
      return;
    }
    try (Statement st = conn.createStatement()) {
      st.execute("drop view if exists " + V_CN + ";");
      st.execute("drop table if exists " + T_ALL + ";");
      st.execute("drop table if exists " + T_CN + ";");
    } catch (Exception ignored) {
      // Every object is prefixed with jdbc_md_cols_; a failed drop must not fail the round.
    } finally {
      try {
        conn.close();
      } catch (Exception ignored) {
        // same reason
      }
    }
  }

  // ------------------------------------------------------------------ helpers

  static List<Map<String, Object>> readAll(ResultSet rs) throws SQLException {
    ResultSetMetaData md = rs.getMetaData();
    int count = md.getColumnCount();
    List<String> names = new ArrayList<>();
    for (int i = 1; i <= count; i++) {
      names.add(md.getColumnName(i));
    }
    List<Map<String, Object>> rows = new ArrayList<>();
    while (rs.next()) {
      Map<String, Object> row = new HashMap<>();
      for (int i = 1; i <= count; i++) {
        row.put(names.get(i - 1), rs.getObject(i));
      }
      rows.add(row);
    }
    rs.close();
    return rows;
  }

  static List<Map<String, Object>> columnsOf(String catalog, String schema, String table,
                                             String columnPattern) throws SQLException {
    return readAll(dmd.getColumns(catalog, schema, table, columnPattern));
  }

  static List<Map<String, Object>> tablesOf(String catalog, String schema, String namePattern,
                                           String[] types) throws SQLException {
    return readAll(dmd.getTables(catalog, schema, namePattern, types));
  }

  static List<Map<String, Object>> schemasOf(String catalog, String schemaPattern)
      throws SQLException {
    return readAll(dmd.getSchemas(catalog, schemaPattern));
  }

  static List<String> valuesOf(List<Map<String, Object>> rows, String column) {
    List<String> out = new ArrayList<>();
    for (Map<String, Object> row : rows) {
      out.add(String.valueOf(row.get(column)));
    }
    Collections.sort(out);
    return out;
  }

  /** The columns of the table as the service stores them: data columns then partitions. */
  static List<Column> storedColumns(String table) throws Exception {
    Table t = odps.tables().get(table);
    t.reload();
    List<Column> all = new ArrayList<>();
    all.addAll(t.getSchema().getColumns());
    all.addAll(t.getSchema().getPartitionColumns());
    return all;
  }

  /** The same columns as the driver describes them when a query reads them back. */
  static ResultSetMetaData driverView(List<Column> columns) {
    List<String> names = new ArrayList<>();
    List<com.aliyun.odps.type.TypeInfo> typeInfos = new ArrayList<>();
    for (Column c : columns) {
      names.add(c.getName());
      typeInfos.add(c.getTypeInfo());
    }
    return new OdpsResultSetMetaData(names, typeInfos);
  }

  // ------------------------------------------------------------------ tests

  @Test
  @DisplayName("getColumns reports the declared precision and scale, like ResultSetMetaData does")
  public void columnSizeAndDigits() throws Exception {
    List<Column> stored = storedColumns(T_ALL);
    ResultSetMetaData rsmd = driverView(stored);
    List<Map<String, Object>> rows = columnsOf(null, null, T_ALL, null);
    Assertions.assertEquals(stored.size(), rows.size(),
        "data columns and partition columns all belong to getColumns()");

    for (int i = 0; i < stored.size(); i++) {
      Column c = stored.get(i);
      Map<String, Object> row = rows.get(i);
      String what = c.getName() + " (" + c.getTypeInfo().getTypeName() + ") ";
      int declaredSize = rsmd.getPrecision(i + 1);
      int declaredDigits = rsmd.getScale(i + 1);

      Assertions.assertEquals(i + 1, ((Number) row.get("ORDINAL_POSITION")).intValue(),
          what + "ORDINAL_POSITION counts from 1 and continues into the partition columns");
      Object size = row.get("COLUMN_SIZE");
      if (declaredSize == Integer.MAX_VALUE) {
        Assertions.assertNull(size,
            what + "is not bounded by MaxCompute, so COLUMN_SIZE stays null instead of claiming "
            + Integer.MAX_VALUE);
      } else {
        Assertions.assertNotNull(size,
            what + "COLUMN_SIZE must carry the declared precision " + declaredSize
            + " that getPrecision() reports");
        Assertions.assertEquals(declaredSize, ((Number) size).intValue(),
            what + "COLUMN_SIZE must equal getPrecision()");
      }
      Assertions.assertEquals(declaredDigits, ((Number) row.get("DECIMAL_DIGITS")).intValue(),
          what + "DECIMAL_DIGITS must equal getScale()");
      Assertions.assertEquals(c.getTypeInfo().getTypeName(), row.get("TYPE_NAME"),
          what + "TYPE_NAME is the MaxCompute type");
    }
  }

  @Test
  @DisplayName("a getColumns row joins to the getTables row of the same table")
  public void identityColumnsJoin() throws Exception {
    List<Map<String, Object>> tables = tablesOf(null, null, PREFIX + "%", null);
    Assertions.assertEquals(3, tables.size(),
        "two tables and one view are listed, got " + valuesOf(tables, "TABLE_NAME"));

    for (Map<String, Object> table : tables) {
      String name = (String) table.get("TABLE_NAME");
      List<Map<String, Object>> columns =
          columnsOf((String) table.get("TABLE_CAT"), (String) table.get("TABLE_SCHEM"), name, null);
      Assertions.assertFalse(columns.isEmpty(), name + ": the catalog has columns");
      for (Map<String, Object> column : columns) {
        Assertions.assertEquals(table.get("TABLE_CAT"), column.get("TABLE_CAT"),
            name + "." + column.get("COLUMN_NAME") + ": TABLE_CAT is the project getTables() "
            + "reported, not the argument of the call");
        Assertions.assertEquals(table.get("TABLE_SCHEM"), column.get("TABLE_SCHEM"),
            name + ": TABLE_SCHEM is the schema getTables() reported");
        Assertions.assertEquals(name, column.get("TABLE_NAME"),
            name + ": TABLE_NAME is the stored name, so the two result sets join");
      }
    }
    Assertions.assertEquals(valuesOf(tables, "TABLE_NAME"),
        valuesOf(tablesOf(project, null, PREFIX + "%", null), "TABLE_NAME"),
        "naming the catalog of this connection lists the same tables");
  }

  @Test
  @DisplayName("types filter the listing; an unknown type matches nothing")
  public void tableTypes() throws Exception {
    List<Map<String, Object>> views = tablesOf(null, null, PREFIX + "%", new String[]{"VIEW"});
    Assertions.assertEquals(Collections.singletonList(V_CN), valuesOf(views, "TABLE_NAME"));
    Assertions.assertEquals(Arrays.asList(T_ALL, T_CN),
        valuesOf(tablesOf(null, null, PREFIX + "%", new String[]{"TABLE"}), "TABLE_NAME"));
    Assertions.assertTrue(tablesOf(null, null, PREFIX + "%", new String[]{"SEQUENCE"}).isEmpty(),
        "a type the service does not have matches no table");
    Assertions.assertEquals(3, valuesOf(tablesOf(null, null, PREFIX + "%",
        new String[]{"TABLE", "VIEW"}), "TABLE_NAME").size());
  }

  @Test
  @DisplayName("columnNamePattern narrows the rows, null keeps them all")
  public void columnNamePattern() throws Exception {
    List<String> stored = new ArrayList<>();
    for (Column c : storedColumns(T_ALL)) {
      stored.add(c.getName());
    }
    Assertions.assertEquals(20, stored.size(), "18 data columns + 2 partition columns");
    Assertions.assertEquals(20, columnsOf(null, null, T_ALL, null).size(),
        "a null columnNamePattern is no criterion at all");
    Assertions.assertEquals(Collections.singletonList("id"),
        valuesOf(columnsOf(null, null, T_ALL, "id"), "COLUMN_NAME"),
        "an exact column name is a criterion too");

    List<String> startsWithD = new ArrayList<>();
    List<String> twoCharsFromD = new ArrayList<>();
    for (String name : stored) {
      if (name.startsWith("d")) {
        startsWithD.add(name);
      }
      if (name.length() == 2 && name.startsWith("d")) {
        twoCharsFromD.add(name);
      }
    }
    Assertions.assertEquals(sorted(startsWithD),
        valuesOf(columnsOf(null, null, T_ALL, "d%"), "COLUMN_NAME"),
        "percent stands for any rest of the name");
    Assertions.assertEquals(sorted(twoCharsFromD),
        valuesOf(columnsOf(null, null, T_ALL, "d_"), "COLUMN_NAME"),
        "underscore stands for exactly one character");
    Assertions.assertTrue(columnsOf(null, null, T_ALL, MISSING).isEmpty(),
        "a column pattern that matches nothing returns an empty result set");

    Assertions.assertEquals("\\", dmd.getSearchStringEscape(),
        "the driver matches patterns with a backslash escape, so it has to say so: a caller that "
        + "needs a literal underscore cannot write one without knowing the escape");
    Assertions.assertEquals(Collections.singletonList("dec_col"),
        valuesOf(columnsOf(null, null, T_ALL, "dec\\_col"), "COLUMN_NAME"),
        "the escape the driver reports is the escape it honours: dec + escaped underscore + col "
        + "is the one column called dec_col, not every 'dec<anything>col'");
  }

  @Test
  @DisplayName("an object that is not there answers with an empty result set")
  public void missingObjects() throws Exception {
    Assertions.assertTrue(columnsOf(null, null, MISSING, null).isEmpty(),
        "getColumns of a table that does not exist returns no rows");
    Assertions.assertTrue(columnsOf(MISSING, null, T_CN, null).isEmpty(),
        "getColumns in a project that does not exist returns no rows");
    Assertions.assertTrue(columnsOf(null, null, T_CN, MISSING).isEmpty(),
        "getColumns of a column that does not exist returns no rows");
    Assertions.assertTrue(tablesOf(MISSING, null, PREFIX + "%", null).isEmpty(),
        "getTables of a project that does not exist returns no rows");
    Assertions.assertTrue(tablesOf(null, MISSING, PREFIX + "%", null).isEmpty(),
        "getTables of a schema that does not exist returns no rows");
    Assertions.assertTrue(tablesOf(null, null, MISSING, null).isEmpty(),
        "getTables of a table name that does not exist returns no rows");
  }

  @Test
  @DisplayName("getSchemas filters, and agrees with getCatalogs about what exists")
  public void schemas() throws Exception {
    List<Map<String, Object>> all = schemasOf(null, null);
    Assertions.assertEquals(1, all.size(), "a two-tier project exposes one implicit schema");
    List<String> catalogNames = valuesOf(readAll(dmd.getCatalogs()),
        OdpsDatabaseMetaData.COL_NAME_TABLE_CAT);
    Assertions.assertEquals(Collections.singletonList(project), catalogNames);
    Assertions.assertEquals(catalogNames, valuesOf(all, "TABLE_CATALOG"),
        "the only schema row sits in the only catalog getCatalogs() admits");
    Assertions.assertEquals(catalogNames, valuesOf(all, "TABLE_SCHEM"),
        "and the schema is named the way getTables()/getColumns() name it -- the two-tier model "
        + "has no other schema to call it");

    Assertions.assertEquals(1, schemasOf(project, null).size(),
        "the catalog of this connection has that schema");
    Assertions.assertTrue(schemasOf(MISSING, null).isEmpty(),
        "a catalog that getCatalogs() does not report has no schemas");
    Assertions.assertTrue(schemasOf(null, MISSING).isEmpty(),
        "a schemaPattern matching nothing returns an empty result set");
    Assertions.assertEquals(1, schemasOf(null, "de_ault").size(),
        "the underscore is a wildcard: de_ault matches default");
    Assertions.assertEquals(1, schemasOf(null, "default").size(),
        "a caller that learned the schema name from an older driver keeps matching: in a "
        + "two-tier project there is only the one implicit schema, whatever it is called");
    Assertions.assertEquals(3, tablesOf(null, "default", PREFIX + "%", null).size(),
        "and that name is still accepted as the schema criterion of getTables()");
    Assertions.assertTrue(schemasOf(null, "zz%").isEmpty(),
        "no two-tier schema name starts with zz");
  }

  @Test
  @DisplayName("a name outside ASCII survives listing and column lookup")
  public void nonAscii() throws Exception {
    List<Map<String, Object>> tables = tablesOf(null, null, T_CN, null);
    Assertions.assertEquals(1, tables.size(), "the table is listed by its exact name");
    Assertions.assertEquals("中文表注释-特殊字符 <>&", tables.get(0).get("REMARKS"),
        "the comment comes back exactly as it was written");
    Assertions.assertEquals(2, columnsOf(null, null, T_CN, null).size());
    Assertions.assertTrue(tablesOf(null, null, "中文%", null).isEmpty(),
        "a pattern of characters no MaxCompute name uses matches nothing, and does not fail");
  }

  @Test
  @DisplayName("an exact name that is not stored that way is a criterion that matched nothing")
  public void mixedCaseLookup() throws Exception {
    // The catalog reports names as the service stores them. Asking in another case must not fail
    // the call; whatever it returns carries the stored name, never the case that was typed.
    List<Map<String, Object>> rows = columnsOf(null, null, T_CN.toUpperCase(), null);
    for (Map<String, Object> row : rows) {
      Assertions.assertEquals(T_CN, row.get("TABLE_NAME"),
          "TABLE_NAME is the stored name, never the case the caller typed");
    }
    Assertions.assertEquals(sorted(Collections.singletonList(T_CN)),
        valuesOf(tablesOf(null, null, T_CN.toUpperCase(), null), "TABLE_NAME"),
        "getTables matches the name case-insensitively and reports it as stored");
  }

  static List<String> sorted(List<String> in) {
    List<String> out = new ArrayList<>(in);
    Collections.sort(out);
    return out;
  }
}
