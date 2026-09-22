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
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;

import com.aliyun.odps.jdbc.data.OdpsArray;
import com.aliyun.odps.jdbc.utils.TestUtils;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;

public class OdpsArrayTest {

    @Test
    public void testOdpsArrayConstructorWithList() throws SQLException {
        // Test creating an OdpsArray with List
        List<Object> arrayData = Arrays.asList("value1", "value2", "value3");
        ArrayTypeInfo arrayTypeInfo = TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.STRING);
        OdpsArray array = new OdpsArray(arrayData, arrayTypeInfo);
        
        // Verify the array
        Assertions.assertEquals("STRING", array.getBaseTypeName());
        Object[] result = (Object[]) array.getArray();
        Assertions.assertArrayEquals(arrayData.toArray(), result);
    }
    
    @Test
    public void testOdpsArrayConstructorWithArray() throws SQLException {
        // Test creating an OdpsArray with Object array
        String[] arrayData = {"value1", "value2", "value3"};
        ArrayTypeInfo arrayTypeInfo = TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.STRING);
        OdpsArray array = new OdpsArray(arrayData, arrayTypeInfo);
        
        // Verify the array
        Assertions.assertEquals("STRING", array.getBaseTypeName());
        Object[] result = (Object[]) array.getArray();
        Assertions.assertArrayEquals(arrayData, result);
    }
    
    @Test
    public void testOdpsArrayWithDifferentTypes() throws SQLException {
        // Test creating array with BIGINT type
        Long[] longArrayData = {1L, 2L, 3L, 4L, 5L};
        ArrayTypeInfo longArrayTypeInfo = TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.BIGINT);
        OdpsArray longArray = new OdpsArray(longArrayData, longArrayTypeInfo);
        
        // Verify the array
        Assertions.assertEquals("BIGINT", longArray.getBaseTypeName());
        Object[] result = (Object[]) longArray.getArray();
        Assertions.assertArrayEquals(longArrayData, result);
        
        // Test creating array with DOUBLE type
        Double[] doubleArrayData = {1.1, 2.2, 3.3};
        ArrayTypeInfo doubleArrayTypeInfo = TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.DOUBLE);
        OdpsArray doubleArray = new OdpsArray(doubleArrayData, doubleArrayTypeInfo);
        
        // Verify the array
        Assertions.assertEquals("DOUBLE", doubleArray.getBaseTypeName());
        Object[] doubleResult = (Object[]) doubleArray.getArray();
        Assertions.assertArrayEquals(doubleArrayData, doubleResult);
    }
    
    @Test
    public void testOdpsArrayGetWithIndexAndCount() throws SQLException {
        // Create an array
        String[] arrayData = {"value1", "value2", "value3", "value4", "value5"};
        ArrayTypeInfo arrayTypeInfo = TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.STRING);
        OdpsArray array = new OdpsArray(arrayData, arrayTypeInfo);
        
        // Test getArray with index and count
        Object[] result = (Object[]) array.getArray(2, 3);
        String[] expected = {"value2", "value3", "value4"};
        Assertions.assertArrayEquals(expected, result);
        
        // Test edge cases
        // Index out of bounds
        Assertions.assertThrows(SQLException.class, () -> array.getArray(0, 3));
        Assertions.assertThrows(SQLException.class, () -> array.getArray(6, 3));
        
        // Negative count
        Assertions.assertThrows(SQLException.class, () -> array.getArray(1, -1));
    }
    
    @Test
    public void testOdpsArrayFree() throws SQLException {
        // Create an array
        String[] arrayData = {"value1", "value2", "value3"};
        ArrayTypeInfo arrayTypeInfo = TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.STRING);
        OdpsArray array = new OdpsArray(arrayData, arrayTypeInfo);
        
        // Test free method
        array.free();
        Assertions.assertNull(array.getArray());
    }
    
    @Test
    public void testOdpsArrayBaseType() throws SQLException {
        // Test getBaseType method
        String[] arrayData = {"value1", "value2", "value3"};
        ArrayTypeInfo arrayTypeInfo = TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.STRING);
        OdpsArray array = new OdpsArray(arrayData, arrayTypeInfo);
        
        // STRING type should map to VARCHAR in JDBC
        Assertions.assertEquals(java.sql.Types.VARCHAR, array.getBaseType());
    }
    
    @Test
    public void testOdpsArrayInvalidConstructor() {
        // Test that constructor throws IllegalArgumentException when parameters are null
        Assertions.assertThrows(IllegalArgumentException.class, () -> 
            new OdpsArray((List<Object>) null, TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.STRING)));
            
        Assertions.assertThrows(IllegalArgumentException.class, () -> 
            new OdpsArray(Arrays.asList("value1", "value2"), null));
            
        Assertions.assertThrows(IllegalArgumentException.class, () -> 
            new OdpsArray((Object[]) null, TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.STRING)));
    }
    
    @Test
    public void testE2EArrayFunctionality() throws Exception {
        Connection conn = TestUtils.getConnection();
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            
            // Drop table if exists
            stmt.executeUpdate("drop table if exists array_e2e_test_table;");
            
            // Create table with array column
            stmt.executeUpdate("create table array_e2e_test_table (id bigint, str_array array<string>);");
            
            // Insert data using SQL directly
            stmt.executeUpdate("insert into array_e2e_test_table values (1, array('apple', 'banana', 'cherry'));");
            stmt.executeUpdate("insert into array_e2e_test_table values (2, array('dog', 'cat', 'bird', 'fish'));");
            
            // Query the data
            PreparedStatement queryStmt = conn.prepareStatement("select * from array_e2e_test_table where id = ?;");
            queryStmt.setLong(1, 1L);
            ResultSet rs = queryStmt.executeQuery();
            
            Assertions.assertTrue(rs.next());
            Assertions.assertEquals(1L, rs.getLong(1));
            
            // Get the array from ResultSet
            java.sql.Array resultArray = rs.getArray(2);
            Assertions.assertNotNull(resultArray);
            Assertions.assertEquals("STRING", resultArray.getBaseTypeName());
            
            // Get array data
            Object[] resultData = (Object[]) resultArray.getArray();
            String[] expected = {"apple", "banana", "cherry"};
            Assertions.assertArrayEquals(expected, resultData);
            
            rs.close();
            queryStmt.close();
            
            // Test with another record
            queryStmt = conn.prepareStatement("select * from array_e2e_test_table where id = ?;");
            queryStmt.setLong(1, 2L);
            rs = queryStmt.executeQuery();
            
            Assertions.assertTrue(rs.next());
            Assertions.assertEquals(2L, rs.getLong(1));
            
            // Get the array from ResultSet
            resultArray = rs.getArray(2);
            Assertions.assertNotNull(resultArray);
            Assertions.assertEquals("STRING", resultArray.getBaseTypeName());
            
            // Get array data
            resultData = (Object[]) resultArray.getArray();
            String[] expected2 = {"dog", "cat", "bird", "fish"};
            Assertions.assertArrayEquals(expected2, resultData);
            
            rs.close();
            queryStmt.close();
            
        } finally {
            if (stmt != null) {
                try {
                    stmt.executeUpdate("drop table if exists array_e2e_test_table;");
                    stmt.close();
                } catch (SQLException e) {
                    // Ignore
                }
            }
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    @Test
    public void testE2EArrayWithDifferentDataTypes() throws Exception {
        Connection conn = TestUtils.getConnection();
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            
            // Drop table if exists
            stmt.executeUpdate("drop table if exists array_types_test_table;");
            
            // Create table with different array columns
            stmt.executeUpdate("create table array_types_test_table (id bigint, " +
                              "bigint_array array<bigint>, " +
                              "double_array array<double>);");
            
            // Insert data using SQL directly
            stmt.executeUpdate("insert into array_types_test_table values " +
                              "(1, array(100, 200, 300), array(1.1, 2.2, 3.3));");
            
            // Query the data
            PreparedStatement queryStmt = conn.prepareStatement("select * from array_types_test_table where id = ?;");
            queryStmt.setLong(1, 1L);
            ResultSet rs = queryStmt.executeQuery();
            
            Assertions.assertTrue(rs.next());
            Assertions.assertEquals(1L, rs.getLong(1));
            
            // Get the bigint array from ResultSet
            java.sql.Array bigintArray = rs.getArray(2);
            Assertions.assertNotNull(bigintArray);
            Assertions.assertEquals("BIGINT", bigintArray.getBaseTypeName());
            
            // Get bigint array data
            Object[] bigintData = (Object[]) bigintArray.getArray();
            Long[] expectedBigint = {100L, 200L, 300L};
            Assertions.assertArrayEquals(expectedBigint, bigintData);
            
            // Get the double array from ResultSet
            java.sql.Array doubleArray = rs.getArray(3);
            Assertions.assertNotNull(doubleArray);
            Assertions.assertEquals("DOUBLE", doubleArray.getBaseTypeName());
            
            // Get double array data
            Object[] doubleData = (Object[]) doubleArray.getArray();
            Double[] expectedDouble = {1.1, 2.2, 3.3};
            Assertions.assertArrayEquals(expectedDouble, doubleData);
            
            rs.close();
            queryStmt.close();
            
        } finally {
            if (stmt != null) {
                try {
                    stmt.executeUpdate("drop table if exists array_types_test_table;");
                    stmt.close();
                } catch (SQLException e) {
                    // Ignore
                }
            }
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    @Test
    public void testE2EArrayGetWithIndexAndCount() throws Exception {
        Connection conn = TestUtils.getConnection();
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            
            // Drop table if exists
            stmt.executeUpdate("drop table if exists array_index_test_table;");
            
            // Create table with array column
            stmt.executeUpdate("create table array_index_test_table (id bigint, str_array array<string>);");
            
            // Insert data using SQL directly
            stmt.executeUpdate("insert into array_index_test_table values (1, array('a', 'b', 'c', 'd', 'e'));");
            
            // Query the data
            PreparedStatement queryStmt = conn.prepareStatement("select * from array_index_test_table where id = ?;");
            queryStmt.setLong(1, 1L);
            ResultSet rs = queryStmt.executeQuery();
            
            Assertions.assertTrue(rs.next());
            Assertions.assertEquals(1L, rs.getLong(1));
            
            // Get the array from ResultSet
            java.sql.Array resultArray = rs.getArray(2);
            Assertions.assertNotNull(resultArray);
            
            // Test getArray with index and count
            Object[] result = (Object[]) resultArray.getArray(2, 3);
            String[] expected = {"b", "c", "d"};
            Assertions.assertArrayEquals(expected, result);
            
            rs.close();
            queryStmt.close();
            
        } finally {
            if (stmt != null) {
                try {
                    stmt.executeUpdate("drop table if exists array_index_test_table;");
                    stmt.close();
                } catch (SQLException e) {
                    // Ignore
                }
            }
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    @Test
    public void testPreparedStatementSetArray() throws Exception {
        Connection conn = TestUtils.getConnection();
        Statement stmt = null;
        try {
            stmt = conn.createStatement();

            // Drop table if exists
            stmt.executeUpdate("drop table if exists ps_array_test_table;");

            // Create table with array column
            stmt.executeUpdate("create table ps_array_test_table (id bigint, str_array array<string>);");

            // Create an array to insert
            String[] arrayData = {"apple", "banana", "cherry"};
            ArrayTypeInfo arrayTypeInfo = TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.STRING);
            OdpsArray odpsArray = new OdpsArray(arrayData, arrayTypeInfo);

            // Insert data using PreparedStatement.setArray()
            PreparedStatement insertStmt = conn.prepareStatement("insert into ps_array_test_table values (?, ?);");
            insertStmt.setLong(1, 1L);
            insertStmt.setArray(2, odpsArray);
            insertStmt.executeUpdate();
            insertStmt.close();

            // Query the data
            PreparedStatement queryStmt = conn.prepareStatement("select * from ps_array_test_table where id = ?;");
            queryStmt.setLong(1, 1L);
            ResultSet rs = queryStmt.executeQuery();

            Assertions.assertTrue(rs.next());
            Assertions.assertEquals(1L, rs.getLong(1));

            // Get the array from ResultSet
            java.sql.Array resultArray = rs.getArray(2);
            Assertions.assertNotNull(resultArray);
            Assertions.assertEquals("STRING", resultArray.getBaseTypeName());

            // Get array data
            Object[] resultData = (Object[]) resultArray.getArray();
            Assertions.assertArrayEquals(arrayData, resultData);

            rs.close();
            queryStmt.close();

        } finally {
            if (stmt != null) {
                try {
                    stmt.executeUpdate("drop table if exists ps_array_test_table;");
                    stmt.close();
                } catch (SQLException e) {
                    // Ignore
                }
            }
            if (conn != null) {
                conn.close();
            }
        }
    }

    @Test
    public void testPreparedStatementSetObjectWithArray() throws Exception {
        Connection conn = TestUtils.getConnection();
        Statement stmt = null;
        try {
            stmt = conn.createStatement();

            // Drop table if exists
            stmt.executeUpdate("drop table if exists ps_object_array_test_table;");

            // Create table with array column
            stmt.executeUpdate("create table ps_object_array_test_table (id bigint, str_array array<string>);");

            // Create an array to insert
            String[] arrayData = {"dog", "cat", "bird"};
            ArrayTypeInfo arrayTypeInfo = TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.STRING);
            OdpsArray odpsArray = new OdpsArray(arrayData, arrayTypeInfo);

            // Insert data using PreparedStatement.setObject()
            PreparedStatement insertStmt = conn.prepareStatement("insert into ps_object_array_test_table values (?, ?);");
            insertStmt.setLong(1, 1L);
            insertStmt.setObject(2, odpsArray);
            insertStmt.executeUpdate();
            insertStmt.close();

            // Query the data
            PreparedStatement queryStmt = conn.prepareStatement("select * from ps_object_array_test_table where id = ?;");
            queryStmt.setLong(1, 1L);
            ResultSet rs = queryStmt.executeQuery();

            Assertions.assertTrue(rs.next());
            Assertions.assertEquals(1L, rs.getLong(1));

            // Get the array from ResultSet
            java.sql.Array resultArray = rs.getArray(2);
            Assertions.assertNotNull(resultArray);
            Assertions.assertEquals("STRING", resultArray.getBaseTypeName());

            // Get array data
            Object[] resultData = (Object[]) resultArray.getArray();
            Assertions.assertArrayEquals(arrayData, resultData);

            rs.close();
            queryStmt.close();

        } finally {
            if (stmt != null) {
                try {
                    stmt.executeUpdate("drop table if exists ps_object_array_test_table;");
                    stmt.close();
                } catch (SQLException e) {
                    // Ignore
                }
            }
            if (conn != null) {
                conn.close();
            }
        }
    }
    /**
     * A generic consumer (BI tool, ORM) reads a column through {@code getObject()} and honours the
     * declared {@code java.sql.Types.ARRAY} mapping. Before the fix the driver returned the raw
     * {@code java.util.ArrayList} produced by the record reader here, so such a cast failed with
     * "class java.util.ArrayList cannot be cast to class java.sql.Array" even though
     * {@code getArray()} had always worked.
     */
    @Test
    public void testSelectArrayGetObjectReturnsSqlArray() throws Exception {
        Connection conn = TestUtils.getConnection();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("select array(1, 2) as big_array, array('x', 'y') as str_array;")) {
            ResultSetMetaData meta = rs.getMetaData();
            Assertions.assertEquals(Types.ARRAY, meta.getColumnType(1));
            Assertions.assertTrue(rs.next(), "expected one result row");

            Object big = rs.getObject(1);
            Assertions.assertTrue(big instanceof Array,
                                  "getObject() on ARRAY<BIGINT> must return java.sql.Array, but was "
                                  + big.getClass().getName());
            Assertions.assertEquals("BIGINT", ((Array) big).getBaseTypeName());
            Assertions.assertEquals("[1, 2]", Arrays.toString((Object[]) ((Array) big).getArray()));

            Object str = rs.getObject(2);
            Assertions.assertTrue(str instanceof Array,
                                  "getObject() on ARRAY<STRING> must return java.sql.Array, but was "
                                  + str.getClass().getName());
            Assertions.assertEquals("[x, y]", Arrays.toString((Object[]) ((Array) str).getArray()));

            // getObject(int, Class) reads the same column, so it must keep working. The wrapper
            // is created per call, so compare contents instead of identity.
            Array typed = rs.getObject(2, Array.class);
            Assertions.assertArrayEquals((Object[]) ((Array) str).getArray(),
                                        (Object[]) typed.getArray());

            // The dedicated accessor stays consistent with getObject().
            Assertions.assertArrayEquals((Object[]) ((Array) big).getArray(),
                                         (Object[]) rs.getArray(1).getArray());
            Assertions.assertFalse(rs.next(), "expected exactly one result row");
        } finally {
            conn.close();
        }
    }

    /**
     * An explicit target type wins over the column's declared mapping: code that names
     * {@code java.util.List} in {@code getObject(int, Class)} keeps getting the list the record
     * reader produced, instead of a {@code java.sql.Array} wrapper it cannot cast.
     */
    @Test
    public void testTypedGetObjectHonoursRequestedType() throws Exception {
        Connection conn = TestUtils.getConnection();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("select array(1, 2) as a, 'hello' as s;")) {
            Assertions.assertTrue(rs.next(), "expected one result row");

            List<?> typedList = rs.getObject(1, List.class);
            Assertions.assertEquals("[1, 2]", typedList.toString(),
                                    "getObject(int, List.class) must keep the raw list");
            Assertions.assertEquals("[1, 2]", rs.getObject("a", List.class).toString(),
                                    "the column-label overload must behave the same");

            // A caller that asks for the standard mapping still gets java.sql.Array.
            Assertions.assertTrue(rs.getObject(1, Array.class) instanceof Array);
            Assertions.assertTrue(rs.getObject(1, Object.class) instanceof Array,
                                  "Object.class is the untyped path and follows Types.ARRAY");

            // Plain JDBC mappings for other columns are untouched by the ARRAY wrapper.
            Assertions.assertEquals("hello", rs.getObject(2, String.class));
            Assertions.assertEquals("hello", rs.getObject(2));
        } finally {
            conn.close();
        }
    }

    /**
     * Escape hatch for applications that relied on the pre-fix raw List from getObject().
     */
    @Test
    public void testLegacyArrayGetObjectKeepsRawList() throws Exception {
        Connection conn = TestUtils.getConnection(
            Collections.singletonMap("legacyArrayGetObject", "true"));
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("select array(1, 2) as a;")) {
            Assertions.assertTrue(rs.next(), "expected one result row");
            Object value = rs.getObject(1);
            Assertions.assertFalse(value instanceof Array,
                                   "legacyArrayGetObject must keep returning the raw List");
            Assertions.assertEquals("[1, 2]", value.toString());
            // getArray() is unaffected by the flag and still returns java.sql.Array.
            Assertions.assertEquals("[1, 2]", Arrays.toString((Object[]) rs.getArray(1).getArray()));
        } finally {
            conn.close();
        }
    }
}