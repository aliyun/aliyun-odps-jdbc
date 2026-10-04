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
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.jdbc.utils.TestUtils;
import com.aliyun.odps.type.TypeInfoFactory;

/**
 * The metadata contract of a three-tier (namespace-schema) project.
 *
 * <p>The catalog is the project, the schema is the schema, and the same table name can exist in
 * two schemas at once -- which is exactly what a two-tier driver never has to face. These cases
 * hold the three listing methods to one reading of the same object:
 * <ul>
 *   <li>{@code getTables(project, schema, name)} lists one row per schema that holds the name, and
 *       {@code getColumns(project, schema, name)} describes that schema's table -- the
 *       {@code TABLE_CAT}/{@code TABLE_SCHEM} of the column rows are the values of the table row
 *       they belong to;</li>
 *   <li>an unrestricted {@code getColumns(catalog, null, name)} describes the table this
 *       connection is sitting in, the same reading {@code getTables} already uses when it
 *       restricts an unrestricted call to the connection schema;</li>
 *   <li>a schema or project that does not exist is an empty result set for all three methods;</li>
 *   <li>{@code getSchemas(catalog, pattern)} reports the schemas {@code getTables} will accept,
 *       and only those that match the pattern.</li>
 * </ul>
 *
 * <p>Fixtures are created through the SDK: a three-part name in SQL is only parsed when the
 * session carries a current schema, and a test must not depend on that. Every call names a
 * schema, because an unrestricted listing enumerates every schema of the project -- minutes on a
 * shared project.
 */
@DisabledIf(value = "com.aliyun.odps.jdbc.utils.ServerCapabilities#namespaceSchemaDisabled",
    disabledReason = "needs the project to run the three-tier model "
        + "(project property odps.schema.model.enabled=true)")
public class MetadataNamespaceContractTest {

  private static final String PREFIX = "jdbc_md_ns_";
  private static final String T_SAME = PREFIX + "same";
  private static final String SCHEMA_A = PREFIX + "a";
  private static final String SCHEMA_B = PREFIX + "b";
  private static final String MISSING = PREFIX + "absent";

  private static Connection conn;
  private static DatabaseMetaData dmd;
  private static Odps odps;
  private static String project;

  @BeforeAll
  public static void setUp() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put("odpsNamespaceSchema", "true");
    conn = TestUtils.getConnection(props);
    dmd = conn.getMetaData();
    odps = TestUtils.getOdps();
    project = odps.getDefaultProject();
    for (String schema : Arrays.asList(SCHEMA_A, SCHEMA_B)) {
      if (!odps.schemas().exists(project, schema)) {
        odps.schemas().create(project, schema);
      }
    }
    create(SCHEMA_A, T_SAME, new Column("id", TypeInfoFactory.BIGINT),
        new Column("val", TypeInfoFactory.getDecimalTypeInfo(10, 2)),
        new Column("only_a", TypeInfoFactory.STRING));
    create(SCHEMA_B, T_SAME, new Column("id", TypeInfoFactory.INT),
        new Column("val", TypeInfoFactory.getCharTypeInfo(5)),
        new Column("only_b", TypeInfoFactory.DOUBLE));
    create("default", T_SAME, new Column("id", TypeInfoFactory.STRING),
        new Column("note", TypeInfoFactory.getVarcharTypeInfo(8)));
  }

  static void create(String schema, String name, Column... columns) throws Exception {
    odps.tables().delete(project, schema, name, true);
    TableSchema ts = new TableSchema();
    for (Column c : columns) {
      ts.addColumn(c);
    }
    Map<String, String> hints = new HashMap<>();
    hints.put("odps.sql.type.system.odps2", "true");
    hints.put("odps.sql.decimal.odps2", "true");
    odps.tables().create(project, schema, name, ts, null, false, null, hints, null);
  }

  @AfterAll
  public static void tearDown() {
    for (String schema : Arrays.asList(SCHEMA_A, SCHEMA_B, "default")) {
      try {
        odps.tables().delete(project, schema, T_SAME, true);
      } catch (Exception ignored) {
        // prefixed objects; a failed drop must not fail the round
      }
    }
    for (String schema : Arrays.asList(SCHEMA_A, SCHEMA_B)) {
      try {
        odps.schemas().delete(project, schema);
      } catch (Exception ignored) {
        // a schema a concurrent run still holds is not this round's failure
      }
    }
    try {
      conn.close();
    } catch (Exception ignored) {
      // same reason
    }
  }

  // ------------------------------------------------------------------ helpers

  static List<Map<String, Object>> columnsOf(String catalog, String schema, String table,
                                             String columnPattern) throws SQLException {
    return MetadataColumnsContractTest.readAll(dmd.getColumns(catalog, schema, table,
        columnPattern));
  }

  static List<Map<String, Object>> tablesOf(String catalog, String schema, String namePattern,
                                            String[] types) throws SQLException {
    return MetadataColumnsContractTest.readAll(
        dmd.getTables(catalog, schema, namePattern, types));
  }

  static List<Map<String, Object>> schemasOf(String catalog, String pattern) throws SQLException {
    return MetadataColumnsContractTest.readAll(dmd.getSchemas(catalog, pattern));
  }

  // ------------------------------------------------------------------ tests

  @Test
  @DisplayName("the same table name in two schemas is described by its own schema")
  public void sameNameInTwoSchemas() throws Exception {
    List<Map<String, Object>> a = columnsOf(project, SCHEMA_A, T_SAME, null);
    List<Map<String, Object>> b = columnsOf(project, SCHEMA_B, T_SAME, null);
    Assertions.assertEquals(3, a.size(), "the schema A table has three columns");
    Assertions.assertEquals(3, b.size(), "the schema B table has three columns");
    Assertions.assertEquals("DECIMAL(10,2)", typeOf(a, "val"), "schema A holds the schema A table");
    Assertions.assertEquals("CHAR(5)", typeOf(b, "val"), "schema B holds the schema B table");
    for (Map<String, Object> row : a) {
      Assertions.assertEquals(SCHEMA_A, row.get("TABLE_SCHEM"), "TABLE_SCHEM of every row");
      Assertions.assertEquals(project, row.get("TABLE_CAT"), "TABLE_CAT of every row");
      Assertions.assertEquals(T_SAME, row.get("TABLE_NAME"));
    }
    Assertions.assertEquals(Collections.singletonList(SCHEMA_B), schemasIn(b),
        "every row of the schema B call names schema B");

    Map<String, Object> dec = row(a, "val");
    Assertions.assertEquals(Integer.valueOf(10), (Integer) dec.get("COLUMN_SIZE"),
        "DECIMAL(10,2) keeps its declared precision through a schema");
    Assertions.assertEquals(2, ((Number) dec.get("DECIMAL_DIGITS")).intValue(),
        "DECIMAL(10,2) keeps its declared scale through a schema");
    Map<String, Object> ch = row(b, "val");
    Assertions.assertEquals(5, ((Number) ch.get("COLUMN_SIZE")).intValue(), "CHAR(5) size");
    Assertions.assertEquals(0, ((Number) ch.get("DECIMAL_DIGITS")).intValue(), "CHAR(5) scale");
  }

  @Test
  @DisplayName("getTables lists one row per schema and each row joins to its own getColumns")
  public void tablesJoinToColumnsAcrossSchemas() throws Exception {
    List<Map<String, Object>> rows = tablesOf(project, PREFIX + "%", T_SAME, null);
    Assertions.assertEquals(Arrays.asList(SCHEMA_A, SCHEMA_B), schemasIn(rows),
        "the two schemas of this round, and no other schema of that prefix, hold the name");

    for (Map<String, Object> table : rows) {
      String schema = (String) table.get("TABLE_SCHEM");
      List<Map<String, Object>> columns =
          columnsOf((String) table.get("TABLE_CAT"), schema, T_SAME, null);
      Assertions.assertEquals(3, columns.size(), "columns listed for " + schema);
      Assertions.assertEquals(Collections.singletonList(schema), schemasIn(columns),
          "the getColumns rows of a getTables row stay in that row's schema");
      Assertions.assertEquals(table.get("TABLE_CAT"), columns.get(0).get("TABLE_CAT"),
          "and in that row's catalog");
    }
  }

  @Test
  @DisplayName("an unrestricted getColumns reads the schema this connection sits in")
  public void unrestrictedColumnsReadsConnectionSchema() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put("odpsNamespaceSchema", "true");
    props.put("schema", SCHEMA_B);
    try (Connection inSchemaB = TestUtils.getConnection(props)) {
      List<Map<String, Object>> rows = readAll(
          inSchemaB.getMetaData().getColumns(null, null, T_SAME, null));
      Assertions.assertEquals(3, rows.size(),
          "the table of the schema this connection sits in has three columns, the table of the "
          + "same name in the default schema has two -- and they are not the same table");
      Assertions.assertEquals(Collections.singletonList(SCHEMA_B), schemasIn(rows),
          "an unrestricted getColumns(catalog, null, name) reads the connection schema, the same "
          + "reading getTables uses when it restricts an unrestricted call to that schema");
    }
  }

  @Test
  @DisplayName("a schema or project that is not there is an empty result set")
  public void missingObjects() throws Exception {
    Assertions.assertTrue(columnsOf(project, MISSING, T_SAME, null).isEmpty(),
        "getColumns in a schema that does not exist returns no rows");
    Assertions.assertTrue(columnsOf(MISSING, SCHEMA_A, T_SAME, null).isEmpty(),
        "getColumns in a project that does not exist returns no rows");
    Assertions.assertTrue(columnsOf(project, SCHEMA_A, MISSING, null).isEmpty(),
        "getColumns of a table that does not exist returns no rows");
    Assertions.assertTrue(tablesOf(project, MISSING, T_SAME, null).isEmpty(),
        "getTables for a schema that does not exist returns no rows");
    Assertions.assertTrue(tablesOf(MISSING, SCHEMA_A, T_SAME, null).isEmpty(),
        "getTables for a project that does not exist returns no rows");
    Assertions.assertTrue(schemasOf(null, MISSING).isEmpty(),
        "getSchemas for a schema pattern that matches nothing returns no rows");
    Assertions.assertTrue(schemasOf(MISSING, null).isEmpty(),
        "getSchemas for a project that does not exist returns no rows");
  }

  @Test
  @DisplayName("getSchemas reports the schemas getTables accepts")
  public void schemaListingIsUsable() throws Exception {
    List<Map<String, Object>> schemas = schemasOf(project, PREFIX + "%");
    Assertions.assertEquals(Arrays.asList(SCHEMA_A, SCHEMA_B), schemasIn(schemas));
    for (Map<String, Object> schema : schemas) {
      Assertions.assertEquals(project, schema.get("TABLE_CATALOG"));
      List<Map<String, Object>> rows =
          tablesOf(project, (String) schema.get("TABLE_SCHEM"), T_SAME, null);
      Assertions.assertEquals(1, rows.size(),
          "the schema getSchemas reported really holds the table");
    }
  }

  // ------------------------------------------------------------------ helpers

  static List<Map<String, Object>> readAll(ResultSet rs) throws SQLException {
    return MetadataColumnsContractTest.readAll(rs);
  }

  static Map<String, Object> row(List<Map<String, Object>> rows, String column) {
    for (Map<String, Object> r : rows) {
      if (column.equals(r.get("COLUMN_NAME"))) {
        return r;
      }
    }
    throw new AssertionError("no column " + column);
  }

  /** The distinct schema names a result set uses, sorted -- a join key, not a per-row value. */
  static List<String> schemasIn(List<Map<String, Object>> rows) {
    List<String> out = new ArrayList<>();
    for (Map<String, Object> r : rows) {
      String schema = String.valueOf(r.get("TABLE_SCHEM"));
      if (!out.contains(schema)) {
        out.add(schema);
      }
    }
    Collections.sort(out);
    return out;
  }

  static String typeOf(List<Map<String, Object>> rows, String column) {
    return (String) row(rows, column).get("TYPE_NAME");
  }
}
