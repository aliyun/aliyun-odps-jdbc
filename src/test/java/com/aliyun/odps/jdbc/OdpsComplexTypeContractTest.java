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
 */

package com.aliyun.odps.jdbc;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aliyun.odps.jdbc.data.OdpsArray;
import com.aliyun.odps.jdbc.data.OdpsStruct;
import com.aliyun.odps.jdbc.utils.TestUtils;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;

/**
 * Contract regressions for the complex types that 3.10.14 left unexplored: nested ARRAY / STRUCT /
 * MAP, empty containers, NULL elements and NULL containers, sub-range reads, and the lifetime of
 * {@link Array} instances around {@code free()} and {@code ResultSet#close()}.
 *
 * <p>Each case states the behaviour the driver actually offers today, so that a change of any of
 * them is a deliberate decision rather than a silent regression. Where that behaviour is a defect
 * the case documents the defect and is marked as such.
 */
public class OdpsComplexTypeContractTest {

    private static final String TABLE = "jdbc_complex_contract_test";

    private static Connection conn;

    @BeforeAll
    public static void setUp() throws Exception {
        conn = TestUtils.getConnection();
        Statement stmt = conn.createStatement();
        // ARRAY/STRUCT/MAP columns need the odps2 type system: declare it instead of inheriting a
        // project default (the same habit OdpsJdbcDateTimeTest adopted for its own precondition).
        stmt.execute("set odps.sql.type.system.odps2=true;");
        // A development leftover from a scratch probe of this same contract, not a product table.
        stmt.executeUpdate("drop table if exists tony_cplx_probe_0924;");
        stmt.executeUpdate("drop table if exists tony_cplx_probe_0924_e;");
        stmt.executeUpdate("drop table if exists " + TABLE + ";");
        stmt.executeUpdate("create table " + TABLE + " (id bigint, "
                           + "aa array<array<string>>, "
                           + "asa array<struct<x:string,y:bigint>>, "
                           + "am array<map<string,bigint>>, "
                           + "st struct<name:string,tags:array<string>,addr:struct<city:string>>, "
                           + "mp map<string,array<bigint>>, "
                           + "a_str array<string>);");

        // id 1: fully populated nested values.
        stmt.executeUpdate("insert into " + TABLE + " values (1, "
                           + "array(array('a','b'), array('c')), "
                           + "array(struct('x1', 10L), struct('x2', 20L)), "
                           + "array(map('k', 1L), map('j', 2L)), "
                           + "struct('John', array('t1','t2'), struct('NY')), "
                           + "map('e', array(1L, 2L)), "
                           + "array('p','q','r','s','t'));");

        // id 2: NULL as the first element, in the middle and at the end, and NULL nested one level
        // down. The leading NULL matters because the type-map path of OdpsArray inspects element 0.
        stmt.executeUpdate("insert into " + TABLE + " values (2, "
                           + "array(array(cast(null as string)), array('c')), "
                           + "array(struct(cast(null as string), cast(null as bigint))), "
                           + "array(map(cast(null as string), cast(null as bigint))), "
                           + "struct(cast(null as string), array(cast(null as string)), struct('LA')), "
                           + "map('e', array(cast(null as bigint))), "
                           + "array(cast(null as string), 'b', 'c', cast(null as string)));");

        // id 3: the whole container is NULL. A bare NULL literal is rejected for complex columns
        // (ODPS-0130071 VOID is not implicitly coercible), so every NULL is cast to its column type.
        stmt.executeUpdate("insert into " + TABLE + " values (3, "
                           + "cast(null as array<array<string>>), "
                           + "cast(null as array<struct<x:string,y:bigint>>), "
                           + "cast(null as array<map<string,bigint>>), "
                           + "cast(null as struct<name:string,tags:array<string>,addr:struct<city:string>>), "
                           + "cast(null as map<string,array<bigint>>), "
                           + "cast(null as array<string>));");

        // id 4: empty containers. SLICE past the end yields an empty array whose element type is
        // the column's, which a bare ARRAY() literal cannot express.
        stmt.executeUpdate("insert into " + TABLE + " values (4, "
                           + "slice(array(array('x')), 3, 2), "
                           + "slice(array(struct('x', 1L)), 3, 2), "
                           + "slice(array(map('k', 1L)), 3, 2), "
                           + "struct('Empty', slice(array('x'), 3, 2), struct('LA')), "
                           + "map('e', slice(array(1L), 3, 2)), "
                           + "slice(array('x'), 3, 2));");
        stmt.close();
    }

    @AfterAll
    public static void tearDown() throws Exception {
        if (conn != null) {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("drop table if exists " + TABLE + ";");
            }
            conn.close();
        }
    }

    private static ArrayTypeInfo nestedStrings() {
        return TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.STRING);
    }

    private static ResultSet query(String columns, long id) throws SQLException {
        PreparedStatement ps = conn.prepareStatement(
            "select " + columns + " from " + TABLE + " where id = ?;");
        ps.setLong(1, id);
        ResultSet rs = ps.executeQuery();
        Assertions.assertTrue(rs.next(), "row id=" + id + " is missing");
        return rs;
    }

    // ----------------------------------------------------------------- nested containers (A1)

    /**
     * A nested ARRAY column is wrapped into {@code java.sql.Array} only at the column level: the
     * elements are the raw values the record reader produced. Consumers that walk the tree with the
     * JDBC {@code Array} API therefore hit a {@code ClassCastException} one level down -- that is
     * the contract of 3.10.14 and this case keeps it visible.
     */
    @Test
    public void testNestedArrayWrapsTheColumnAndOnlyTheColumn() throws Exception {
        ResultSet rs = query("aa, asa, am", 1L);

        Array nested = rs.getArray(1);
        Assertions.assertEquals("ARRAY<STRING>", nested.getBaseTypeName());
        Assertions.assertEquals(Types.ARRAY, nested.getBaseType());
        Object[] elements = (Object[]) nested.getArray();
        Assertions.assertEquals(2, elements.length);
        Assertions.assertEquals(Arrays.asList("a", "b"), elements[0]);
        Assertions.assertEquals(Collections.singletonList("c"), elements[1]);
        Assertions.assertFalse(elements[0] instanceof Array,
                               "nested ARRAY elements are not wrapped in java.sql.Array");
        Assertions.assertThrows(ClassCastException.class, () -> ((Array) elements[0]).getArray());

        Array structs = rs.getArray(2);
        Assertions.assertEquals("STRUCT<x:STRING,y:BIGINT>", structs.getBaseTypeName());
        Assertions.assertEquals(Types.STRUCT, structs.getBaseType());
        Object[] structElements = (Object[]) structs.getArray();
        Assertions.assertEquals(2, structElements.length);
        Assertions.assertTrue(structElements[0] instanceof com.aliyun.odps.data.Struct,
                              "ARRAY<STRUCT> elements keep the SDK struct representation");
        Assertions.assertEquals("x2", ((com.aliyun.odps.data.Struct) structElements[1])
            .getFieldValue("x"));

        Array maps = rs.getArray(3);
        Assertions.assertEquals("MAP<STRING,BIGINT>", maps.getBaseTypeName());
        Assertions.assertEquals(Types.JAVA_OBJECT, maps.getBaseType());
        Object[] mapElements = (Object[]) maps.getArray();
        Assertions.assertEquals(2, mapElements.length);
        Assertions.assertTrue(mapElements[0] instanceof Map);
        Assertions.assertEquals(1L, ((Map<String, Object>) mapElements[0]).get("k"));
        rs.close();
    }

    /**
     * STRUCT and MAP columns are read through the untyped and typed {@code getObject} paths, and
     * their nested ARRAY fields/values are again raw. A MAP column is advertised as
     * JAVA_OBJECT: it has no JDBC mapping, and asking for it with {@code getArray} must be a
     * SQLException rather than a cast failure or a NULL.
     */
    @Test
    public void testStructAndMapColumnsKeepTheirMapping() throws Exception {
        ResultSet rs = query("st, mp, aa", 1L);
        ResultSetMetaData md = rs.getMetaData();
        Assertions.assertEquals(Types.STRUCT, md.getColumnType(1));
        Assertions.assertEquals(Types.JAVA_OBJECT, md.getColumnType(2));
        Assertions.assertEquals(Types.ARRAY, md.getColumnType(3));

        Object structValue = rs.getObject(1);
        Assertions.assertTrue(structValue instanceof com.aliyun.odps.data.Struct);
        Assertions.assertEquals("John", ((com.aliyun.odps.data.Struct) structValue)
            .getFieldValue("name"));
        Assertions.assertEquals(Arrays.asList("t1", "t2"),
                                ((com.aliyun.odps.data.Struct) structValue).getFieldValue("tags"));
        Assertions.assertFalse(((List<?>) ((com.aliyun.odps.data.Struct) structValue)
                                   .getFieldValue("tags")).get(0) instanceof Array,
                               "an ARRAY inside a STRUCT field is not wrapped either");
        Assertions.assertTrue(rs.getString(1).contains("NY"));

        Object mapValue = rs.getObject(2);
        Assertions.assertTrue(mapValue instanceof Map);
        Assertions.assertEquals(Arrays.asList(1L, 2L), ((Map<String, Object>) mapValue).get("e"));
        Assertions.assertTrue(rs.getString(2).contains("e"));

        Assertions.assertThrows(SQLException.class, () -> rs.getArray(1),
                                "getArray on a STRUCT column is a SQLException");
        Assertions.assertThrows(SQLException.class, () -> rs.getArray(2),
                                "getArray on a MAP column is a SQLException, not a NULL");
        rs.close();
    }

    /** The declared column types of nested containers, as a BI tool sees them. */
    @Test
    public void testMetadataOfNestedColumns() throws Exception {
        PreparedStatement ps = conn.prepareStatement(
            "select aa, asa, am, st, mp, a_str from " + TABLE + " where id = 1;");
        ResultSet rs = ps.executeQuery();
        ResultSetMetaData md = rs.getMetaData();
        Assertions.assertEquals("ARRAY<ARRAY<STRING>>", md.getColumnTypeName(1));
        Assertions.assertEquals("ARRAY<STRUCT<x:STRING,y:BIGINT>>", md.getColumnTypeName(2));
        Assertions.assertEquals("ARRAY<MAP<STRING,BIGINT>>", md.getColumnTypeName(3));
        Assertions.assertEquals(Types.ARRAY, md.getColumnType(1));
        Assertions.assertEquals(Types.ARRAY, md.getColumnType(2));
        Assertions.assertEquals(Types.ARRAY, md.getColumnType(3));
        Assertions.assertEquals(Types.STRUCT, md.getColumnType(4));
        Assertions.assertEquals(Types.JAVA_OBJECT, md.getColumnType(5));
        Assertions.assertEquals(Types.ARRAY, md.getColumnType(6));
        rs.close();
        ps.close();
    }

    // ------------------------------------------------------- empty containers and NULL elements (A1)

    /**
     * A NULL <em>element</em> is not the same thing as a NULL container: the position and the
     * nullness of each element survive the read, {@code wasNull()} only describes the whole column.
     */
    @Test
    public void testNullElementsKeepTheirPosition() throws Exception {
        ResultSet rs = query("a_str, aa", 2L);

        Array withNulls = rs.getArray(1);
        Object[] elements = (Object[]) withNulls.getArray();
        Assertions.assertEquals(4, elements.length);
        Assertions.assertNull(elements[0], "NULL as the first element");
        Assertions.assertEquals("b", elements[1]);
        Assertions.assertEquals("c", elements[2]);
        Assertions.assertNull(elements[3], "NULL as the last element");
        Assertions.assertFalse(rs.wasNull(), "the column itself is not NULL");

        Array nested = rs.getArray(2);
        Object[] inner = (Object[]) nested.getArray();
        Assertions.assertEquals(Collections.singletonList(null), inner[0]);
        Assertions.assertEquals(Collections.singletonList("c"), inner[1]);
        rs.close();
    }

    /**
     * Reading NULL elements through the {@code Map} overloads used to inspect element 0 only, and a
     * NULL there threw a NullPointerException. The mapping is a check against the element class, so
     * it is decided by the first element that actually has one.
     */
    @Test
    public void testNullElementsDoNotBreakTheTypeMapPaths() throws Exception {
        ArrayTypeInfo strings = TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.STRING);

        Array withLeadingNull = new OdpsArray(Arrays.asList(null, "b"), strings);
        Map<String, Class<?>> stringMap = new HashMap<>();
        stringMap.put("STRING", String.class);
        Assertions.assertEquals(2, ((Object[]) withLeadingNull.getArray(stringMap)).length);
        Assertions.assertEquals("b", ((Object[]) withLeadingNull.getArray(2, 1, stringMap))[0]);

        // A mapping that contradicts a non-NULL element is still refused, with a SQLException.
        Map<String, Class<?>> wrongMap = new HashMap<>();
        wrongMap.put("STRING", Long.class);
        Assertions.assertThrows(SQLException.class, () -> withLeadingNull.getArray(wrongMap));

        // An all-NULL array has nothing to contradict any mapping and is returned as it is.
        Array allNull = new OdpsArray(Arrays.<Object>asList(null, null), strings);
        Assertions.assertEquals(2, ((Object[]) allNull.getArray(stringMap)).length);
    }

    // --------------------------------------------------------------- sub-ranges (non-zero window)

    /**
     * {@code getArray(index, count)} is 1-based, clamps to the remaining elements, and a leading
     * non-zero index must be honoured by the {@code Map} overload as well -- it used to hand back
     * the whole array whenever the map matched.
     */
    @Test
    public void testSubRangeWindows() throws Exception {
        ResultSet rs = query("a_str, aa", 1L);

        Array flat = rs.getArray(1);
        Assertions.assertArrayEquals(new Object[] {"q"}, (Object[]) flat.getArray(2, 1));
        Assertions.assertArrayEquals(new Object[] {"q", "r", "s"}, (Object[]) flat.getArray(2, 3));
        // clamped to what is left
        Assertions.assertArrayEquals(new Object[] {"s", "t"}, (Object[]) flat.getArray(4, 99));
        // empty window at a valid index
        Assertions.assertEquals(0, ((Object[]) flat.getArray(2, 0)).length);
        Assertions.assertThrows(SQLException.class, () -> flat.getArray(0, 1));
        Assertions.assertThrows(SQLException.class, () -> flat.getArray(6, 1));
        Assertions.assertThrows(SQLException.class, () -> flat.getArray(1, -1));

        Array nested = rs.getArray(2);
        Object[] window = (Object[]) nested.getArray(2, 1);
        Assertions.assertEquals(1, window.length);
        Assertions.assertEquals(Collections.singletonList("c"), window[0]);
        rs.close();
    }

    @Test
    public void testSubRangeIsHonouredByTheMapOverloads() throws Exception {
        ArrayTypeInfo strings = TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.STRING);
        Array a = new OdpsArray(new Object[] {"a", "b", "c", "d", "e"}, strings);
        Map<String, Class<?>> stringMap = new HashMap<>();
        stringMap.put("STRING", String.class);

        Assertions.assertArrayEquals(new Object[] {"b", "c"}, (Object[]) a.getArray(2, 2, stringMap));
        Assertions.assertArrayEquals(new Object[] {"b", "c"}, (Object[]) a.getArray(2, 2));
        // without a binding for the base type the window must still be the window
        Assertions.assertArrayEquals(new Object[] {"d", "e"}, (Object[]) a.getArray(4, 5, null));
        Assertions.assertEquals(5, ((Object[]) a.getArray(stringMap)).length);
    }

    // ------------------------------------------------------------------ NULL containers (A2)

    /**
     * A NULL ARRAY column read through {@code getArray} is a NULL value, not a type error: it
     * returned a NullPointerException (and after the fix it must never be anything but null),
     * exactly like the untyped {@code getObject} path already did.
     */
    @Test
    public void testNullContainerReadsAsNull() throws Exception {
        ResultSet rs = query("a_str, aa, st, mp", 3L);

        Assertions.assertNull(rs.getObject(1));
        Assertions.assertTrue(rs.wasNull());
        Assertions.assertNull(rs.getArray(1),
                              "getArray() of a NULL ARRAY column is null, like every other accessor");
        Assertions.assertNull(rs.getArray("a_str"), "the same by column label");
        Assertions.assertNull(rs.getString(1));
        Assertions.assertNull(rs.getObject(1, List.class));
        Assertions.assertNull(rs.getObject(1, Array.class));

        Assertions.assertNull(rs.getObject(2));
        Assertions.assertNull(rs.getObject(3), "NULL STRUCT column");
        Assertions.assertNull(rs.getObject(4), "NULL MAP column");
        rs.close();
    }

    /** An empty container is zero length, never null, and never reported as SQL NULL. */
    @Test
    public void testEmptyContainers() throws Exception {
        ArrayTypeInfo strings = TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.STRING);
        Array empty = new OdpsArray(Collections.<Object>emptyList(), strings);
        Assertions.assertEquals(0, ((Object[]) empty.getArray()).length);
        Assertions.assertEquals("STRING", empty.getBaseTypeName());
        Assertions.assertEquals(Types.VARCHAR, empty.getBaseType());
        Assertions.assertThrows(SQLException.class, () -> empty.getArray(1, 1),
                                "index 1 of an empty array is out of bounds");
        Map<String, Class<?>> stringMap = new HashMap<>();
        stringMap.put("STRING", String.class);
        Assertions.assertEquals(0, ((Object[]) empty.getArray(stringMap)).length);
        // There is no valid 1-based index into an empty array, with or without a type map.
        Assertions.assertThrows(SQLException.class, () -> empty.getArray(1, 0, stringMap));

        Array emptyNested = new OdpsArray(Collections.<Object>emptyList(),
                                          TypeInfoFactory.getArrayTypeInfo(nestedStrings()));
        Assertions.assertEquals("ARRAY<STRING>", emptyNested.getBaseTypeName());
        Assertions.assertEquals(Types.ARRAY, emptyNested.getBaseType());
        Assertions.assertEquals(0, ((Object[]) emptyNested.getArray()).length);
    }

    /**
     * Empty containers as they come back from a real table: zero length, never null, and never
     * reported as SQL NULL -- an application that distinguishes "no elements" from "no value" has
     * to be able to tell them apart.
     */
    @Test
    public void testEmptyContainersRoundTripThroughTheServer() throws Exception {
        ResultSet rs = query("a_str, aa, st, mp", 4L);

        Array empty = rs.getArray(1);
        Assertions.assertNotNull(empty, "an empty ARRAY column is not a NULL value");
        Assertions.assertEquals(0, ((Object[]) empty.getArray()).length);
        Assertions.assertEquals("STRING", empty.getBaseTypeName());
        Assertions.assertTrue(rs.getObject(1) instanceof List, "the legacy path keeps the raw List");
        Assertions.assertEquals(0, ((List<?>) rs.getObject(1)).size());
        Assertions.assertFalse(rs.wasNull(), "an empty ARRAY is not SQL NULL");
        Assertions.assertThrows(SQLException.class, () -> empty.getArray(1, 1),
                                "an empty array has no valid 1-based index");

        Array emptyNested = rs.getArray(2);
        Assertions.assertEquals("ARRAY<STRING>", emptyNested.getBaseTypeName());
        Assertions.assertEquals(0, ((Object[]) emptyNested.getArray()).length);

        com.aliyun.odps.data.Struct st = (com.aliyun.odps.data.Struct) rs.getObject(3);
        Assertions.assertEquals("Empty", st.getFieldValue("name"));
        Assertions.assertEquals(0, ((List<?>) st.getFieldValue("tags")).size());
        Assertions.assertEquals(0, ((List<?>) ((Map<String, Object>) rs.getObject(4)).get("e")).size());
        rs.close();
    }

    // -------------------------------------------------------------- lifetime of java.sql.Array (A2)

    /**
     * {@code free()} releases the values. The zero-argument read then returns null -- the shape
     * {@code OdpsArrayTest#testOdpsArrayFree} pinned in 3.10.14 -- while the sub-range and type-map
     * reads used to dereference the dropped state and raise a NullPointerException. The metadata of
     * the column stays readable because it describes the column, not the released values.
     */
    @Test
    public void testAccessAfterFreeIsNotANullPointerException() throws Exception {
        ArrayTypeInfo strings = TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.STRING);
        Array a = new OdpsArray(new Object[] {"x", "y"}, strings);
        a.free();

        Assertions.assertNull(a.getArray());
        Assertions.assertNull(a.getArray(1, 1), "a freed array has no values left to window over");
        Assertions.assertNull(a.getArray(Collections.<String, Class<?>>emptyMap()));
        Assertions.assertNull(a.getArray(1, 1, Collections.<String, Class<?>>emptyMap()));
        Assertions.assertEquals("STRING", a.getBaseTypeName());
        Assertions.assertEquals(Types.VARCHAR, a.getBaseType());
        // free() is idempotent
        a.free();
    }

    /**
     * The same instance read from a real {@code ResultSet}: freeing the wrapper must not leak an
     * NPE into the caller.
     */
    @Test
    public void testAccessAfterFreeOnAResultSetArray() throws Exception {
        ResultSet rs = query("a_str", 1L);
        Array a = rs.getArray(1);
        Assertions.assertEquals(5, ((Object[]) a.getArray()).length);
        a.free();
        Assertions.assertNull(a.getArray());
        Assertions.assertNull(a.getArray(1, 2), "a freed array read from a ResultSet gives no values");
        Assertions.assertEquals("STRING", a.getBaseTypeName());
        rs.close();
    }

    /**
     * Closing the {@code ResultSet} (or its {@code Statement}) does not invalidate an Array taken
     * from it: the wrapper owns a copy of the row's value. That is what lets a caller hand an Array
     * to code that runs after the cursor moved on.
     */
    @Test
    public void testArrayOutlivesItsResultSet() throws Exception {
        PreparedStatement ps = conn.prepareStatement(
            "select a_str, aa from " + TABLE + " where id = 1;");
        ResultSet rs = ps.executeQuery();
        Assertions.assertTrue(rs.next());
        Array flat = rs.getArray(1);
        Array nested = rs.getArray(2);
        rs.close();

        Assertions.assertEquals(5, ((Object[]) flat.getArray()).length);
        Assertions.assertEquals(2, ((Object[]) nested.getArray()).length);
        Assertions.assertEquals(Collections.singletonList("c"), ((Object[]) nested.getArray())[1]);

        // The values are a snapshot: the statement can close too, and freeing is still explicit.
        ps.close();
        Assertions.assertArrayEquals(new Object[] {"p", "q", "r", "s", "t"},
                                    (Object[]) flat.getArray());
    }

    // ------------------------------------------------- legacy / standard ARRAY getObject switch (A2)

    /** Untyped {@code getObject} on a nested column with the default (legacy) connection. */
    @Test
    public void testLegacyGetObjectKeepsRawListForNestedColumns() throws Exception {
        ResultSet rs = query("aa, a_str", 1L);
        Assertions.assertTrue(rs.getObject(1) instanceof List,
                              "legacy_array_get_object defaults to true: the raw List");
        Assertions.assertTrue(rs.getObject(2) instanceof List);
        Assertions.assertFalse(rs.getObject(1) instanceof Array);
        // Naming the type wins over the default mapping, and it survives the legacy flag.
        Assertions.assertTrue(rs.getObject(1, Array.class) instanceof Array);
        Assertions.assertTrue(rs.getObject(1, List.class) instanceof List);
        Object[] elements = (Object[]) ((Array) rs.getObject(1, Array.class)).getArray();
        Assertions.assertEquals(Arrays.asList("a", "b"), elements[0]);
        rs.close();
    }

    /** The same columns with {@code legacy_array_get_object=false}. */
    @Test
    public void testStandardGetObjectWrapsNestedColumns() throws Exception {
        Connection standard = TestUtils.getConnection(
            Collections.singletonMap("legacy_array_get_object", "false"));
        try {
            PreparedStatement ps = standard.prepareStatement(
                "select aa, a_str from " + TABLE + " where id = 1;");
            ResultSet rs = ps.executeQuery();
            Assertions.assertTrue(rs.next());
            Assertions.assertTrue(rs.getObject(1) instanceof Array,
                                  "opting out of the legacy mapping gives java.sql.Array");
            Assertions.assertTrue(rs.getObject(2) instanceof Array);
            // A caller that names java.util.List is still served the raw value.
            Assertions.assertTrue(rs.getObject(1, List.class) instanceof List);
            Assertions.assertFalse(rs.getObject(1, List.class) instanceof Array);
            Object[] elements = (Object[]) ((Array) rs.getObject(1)).getArray();
            Assertions.assertEquals(Collections.singletonList("c"), elements[1]);
            rs.close();
            ps.close();
        } finally {
            standard.close();
        }
    }

    /**
     * A {@code Struct} column asked for as {@code java.sql.Struct}: the typed path converts the
     * SDK struct, the nested ARRAY attribute stays raw.
     */
    @Test
    public void testTypedStructRequestKeepsNestedArraysRaw() throws Exception {
        ResultSet rs = query("st", 1L);
        Object typed = rs.getObject(1, java.sql.Struct.class);
        Assertions.assertTrue(typed instanceof OdpsStruct,
                              "Types.STRUCT advertises java.sql.Struct and the typed path delivers it");
        Object[] attributes = ((java.sql.Struct) typed).getAttributes();
        Assertions.assertEquals("John", attributes[0]);
        Assertions.assertEquals(Arrays.asList("t1", "t2"), attributes[1]);
        Assertions.assertFalse(attributes[1] instanceof Array);
        rs.close();
    }
}
