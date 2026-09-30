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

import java.util.Properties;
import org.junit.jupiter.api.Test;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;
import com.aliyun.odps.jdbc.utils.OdpsLogger;
import com.aliyun.odps.sqa.SQLExecutor;
import com.aliyun.odps.sqa.SQLExecutorBuilder;

class InstancePriorityPropagationTest {
  @Test void standaloneAndPerQuerySettingsReachSqlExecutor() throws Exception {
    OdpsConnection connection = mock(OdpsConnection.class);
    connection.log = mock(OdpsLogger.class);
    SQLExecutor executor = mock(SQLExecutor.class);
    SQLExecutorBuilder builder = mock(SQLExecutorBuilder.class);
    when(builder.clone()).thenReturn(builder);
    when(connection.getExecutorBuilder()).thenReturn(builder);
    when(connection.getExecutor()).thenReturn(executor);
    when(connection.getSqlTaskProperties()).thenReturn(new Properties());
    OdpsStatement statement = new OdpsStatement(connection);
    statement.executeUpdate("set odps.instance.priority=3;");
    statement.executeUpdate("insert into example values (1);");
    verify(executor).run(eq("insert into example values (1);"), argThat(hints -> "3".equals(hints.get("odps.instance.priority"))));
    statement.executeUpdate("set odps.instance.priority=5; insert into example values (2);");
    verify(executor).run(contains("values (2)"), argThat(hints -> "5".equals(hints.get("odps.instance.priority"))));
  }
}
