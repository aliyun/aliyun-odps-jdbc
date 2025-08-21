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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;

import com.aliyun.odps.jdbc.data.OdpsStruct;
import com.aliyun.odps.jdbc.utils.TestUtils;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfoParser;

public class OdpsStructTest {

    @Test
    public void testOdpsStructConstructor() throws SQLException {
        // Test creating an OdpsStruct with attributes
        Object[] attributes = {"value1", "value2", "value3"};
        String typeName = "STRUCT<a:STRING,b:STRING,c:STRING>";
        
        // Create TypeInfo for the struct
        OdpsStruct struct = new OdpsStruct(attributes,
                                           (StructTypeInfo) TypeInfoParser.getTypeInfoFromTypeString(
                                               typeName));
        
        // Verify the struct
        Assertions.assertEquals(typeName, struct.getSQLTypeName());
        Object[] result = struct.getAttributes();
        Assertions.assertArrayEquals(attributes, result);
    }
    
    @Test
    public void testOdpsStructGetAttributesWithMap() throws SQLException {
        // Test creating an OdpsStruct with attributes
        Object[] attributes = {"value1", "value2", "value3"};
        String typeName = "STRUCT<a:STRING,b:STRING,c:STRING>";
        
        // Create TypeInfo for the struct
        OdpsStruct struct = new OdpsStruct(attributes,
                           (StructTypeInfo) TypeInfoParser.getTypeInfoFromTypeString(typeName));
        
        // Test getAttributes with map - should throw SQLFeatureNotSupportedException
        Map<String, Class<?>> map = new HashMap<>();
        Assertions.assertThrows(SQLException.class, () -> {
            struct.getAttributes(map);
        });
    }
    
    @Test
    public void testE2EStructFunctionality() throws Exception {
        Connection conn = TestUtils.getConnection();
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            
            // Drop table if exists
            stmt.executeUpdate("drop table if exists struct_e2e_test_table;");
            
            // Create table with struct column
            stmt.executeUpdate("create table struct_e2e_test_table (id bigint, person struct<name:string,age:int>);");
            
            // Insert data using SQL directly
            stmt.executeUpdate("insert into struct_e2e_test_table values (1, struct('Alice', 25));");
            stmt.executeUpdate("insert into struct_e2e_test_table values (2, struct('Bob', 30));");
            
            // Query the data
            PreparedStatement queryStmt = conn.prepareStatement("select * from struct_e2e_test_table where id = ?;");
            queryStmt.setLong(1, 1L);
            ResultSet rs = queryStmt.executeQuery();
            
            Assertions.assertTrue(rs.next());
            Assertions.assertEquals(1L, rs.getLong(1));
            
            // Get the struct from ResultSet (as string since JDBC doesn't have native struct support)
            String resultStruct = rs.getString(2);
            Assertions.assertNotNull(resultStruct);
            Assertions.assertTrue(resultStruct.contains("Alice") && resultStruct.contains("25"));
            
            rs.close();
            queryStmt.close();
            
            // Test with another record
            queryStmt = conn.prepareStatement("select * from struct_e2e_test_table where id = ?;");
            queryStmt.setLong(1, 2L);
            rs = queryStmt.executeQuery();
            
            Assertions.assertTrue(rs.next());
            Assertions.assertEquals(2L, rs.getLong(1));
            
            // Get the struct from ResultSet
            resultStruct = rs.getString(2);
            Assertions.assertNotNull(resultStruct);
            Assertions.assertTrue(resultStruct.contains("Bob") && resultStruct.contains("30"));
            
            rs.close();
            queryStmt.close();
            
        } finally {
            if (stmt != null) {
                try {
                    stmt.executeUpdate("drop table if exists struct_e2e_test_table;");
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
    public void testE2EStructWithComplexTypes() throws Exception {
        Connection conn = TestUtils.getConnection();
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            
            // Drop table if exists
            stmt.executeUpdate("drop table if exists struct_complex_test_table;");
            
            // Create table with complex struct column
            stmt.executeUpdate("create table struct_complex_test_table (id bigint, " +
                              "employee struct<name:string,age:int,salary:double,department:string>);");
            
            // Insert data using SQL directly
            stmt.executeUpdate("insert into struct_complex_test_table values " +
                              "(1, struct('John Doe', 35, 75000.50, 'Engineering'));");
            
            // Query the data
            PreparedStatement queryStmt = conn.prepareStatement("select * from struct_complex_test_table where id = ?;");
            queryStmt.setLong(1, 1L);
            ResultSet rs = queryStmt.executeQuery();
            
            Assertions.assertTrue(rs.next());
            Assertions.assertEquals(1L, rs.getLong(1));
            
            // Get the struct from ResultSet (as string)
            String resultStruct = rs.getString(2);
            Assertions.assertNotNull(resultStruct);
            Assertions.assertTrue(resultStruct.contains("John Doe") && 
                                resultStruct.contains("35") && 
                                resultStruct.contains("75000.5") && 
                                resultStruct.contains("Engineering"));
            
            rs.close();
            queryStmt.close();
            
        } finally {
            if (stmt != null) {
                try {
                    stmt.executeUpdate("drop table if exists struct_complex_test_table;");
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
    public void testE2EStructWithNestedStructs() throws Exception {
        Connection conn = TestUtils.getConnection();
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            
            // Drop table if exists
            stmt.executeUpdate("drop table if exists struct_nested_test_table;");
            
            // Create table with nested struct column
            stmt.executeUpdate("create table struct_nested_test_table (id bigint, " +
                              "person struct<name:string,address:struct<street:string,city:string>>);");
            
            // Insert data using SQL directly
            stmt.executeUpdate("insert into struct_nested_test_table values " +
                              "(1, struct('Alice', struct('123 Main St', 'New York')));");
            
            // Query the data
            PreparedStatement queryStmt = conn.prepareStatement("select * from struct_nested_test_table where id = ?;");
            queryStmt.setLong(1, 1L);
            ResultSet rs = queryStmt.executeQuery();
            
            Assertions.assertTrue(rs.next());
            Assertions.assertEquals(1L, rs.getLong(1));
            
            // Get the struct from ResultSet (as string)
            String resultStruct = rs.getString(2);
            Assertions.assertNotNull(resultStruct);
            Assertions.assertTrue(resultStruct.contains("Alice") && 
                                resultStruct.contains("123 Main St") && 
                                resultStruct.contains("New York"));
            
            rs.close();
            queryStmt.close();
            
        } finally {
            if (stmt != null) {
                try {
                    stmt.executeUpdate("drop table if exists struct_nested_test_table;");
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
    public void testPreparedStatementSetObjectWithStruct() throws Exception {
        Connection conn = TestUtils.getConnection();
        Statement stmt = null;
        try {
            stmt = conn.createStatement();

            // Drop table if exists
            stmt.executeUpdate("drop table if exists ps_struct_test_table;");

            // Create table with struct column
            stmt.executeUpdate("create table ps_struct_test_table (id bigint, person struct<name:string,age:int>);");

            // Create a struct to insert
            Object[] attributes = {"John Doe", 30};
            String typeName = "STRUCT<name:STRING,age:INT>";
            StructTypeInfo structTypeInfo = (StructTypeInfo) TypeInfoParser.getTypeInfoFromTypeString(typeName);
            OdpsStruct odpsStruct = new OdpsStruct(attributes, structTypeInfo);

            // Insert data using PreparedStatement.setObject()
            PreparedStatement insertStmt = conn.prepareStatement("insert into ps_struct_test_table values (?, ?);");
            insertStmt.setLong(1, 1L);
            insertStmt.setObject(2, odpsStruct);
            insertStmt.executeUpdate();
            insertStmt.close();

            // Query the data
            PreparedStatement queryStmt = conn.prepareStatement("select * from ps_struct_test_table where id = ?;");
            queryStmt.setLong(1, 1L);
            ResultSet rs = queryStmt.executeQuery();

            Assertions.assertTrue(rs.next());
            Assertions.assertEquals(1L, rs.getLong(1));

            // Get the struct from ResultSet (as string)
            String resultStruct = rs.getString(2);
            Assertions.assertNotNull(resultStruct);
            Assertions.assertTrue(resultStruct.contains("John Doe") && resultStruct.contains("30"));

            rs.close();
            queryStmt.close();

        } finally {
            if (stmt != null) {
                try {
                    stmt.executeUpdate("drop table if exists ps_struct_test_table;");
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
}