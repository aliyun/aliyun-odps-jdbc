/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.jdbc.utils;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Project;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ServerCapabilitiesTest {
  private Odps projectWithProperty(String value) throws Exception {
    Odps odps = mock(Odps.class, RETURNS_DEEP_STUBS);
    when(odps.getDefaultProject()).thenReturn("capability_test");
    when(odps.projects().get("capability_test")
        .getProperty(ServerCapabilities.NAMESPACE_SCHEMA_PROPERTY)).thenReturn(value);
    return odps;
  }

  @Test
  void explicitPropertyControlsTheGate() throws Exception {
    assertTrue(ServerCapabilities.namespaceSchemaEnabled(projectWithProperty(" true ")));
    assertFalse(ServerCapabilities.namespaceSchemaEnabled(projectWithProperty("false")));
  }

  @Test
  void missingPropertyFallsBackToSchemaProbe() throws Exception {
    Odps odps = projectWithProperty(null);
    when(odps.schemas().exists("default")).thenReturn(true);
    assertTrue(ServerCapabilities.namespaceSchemaEnabled(odps));
    when(odps.schemas().exists("default")).thenReturn(false);
    assertFalse(ServerCapabilities.namespaceSchemaEnabled(odps));
  }

  @Test
  void projectAccessFailureMustFailInsteadOfSkipping() throws Exception {
    Odps odps = projectWithProperty("true");
    Project project = odps.projects().get("capability_test");
    OdpsException failure = new OdpsException("project access denied");
    doThrow(failure).when(project).reload();
    assertSame(failure, assertThrows(OdpsException.class,
        () -> ServerCapabilities.namespaceSchemaEnabled(odps)));
  }

  @Test
  void schemaProbeFailureMustFailInsteadOfSkipping() throws Exception {
    Odps odps = projectWithProperty(null);
    OdpsException failure = new OdpsException("schema endpoint unavailable");
    when(odps.schemas().exists("default")).thenThrow(failure);
    assertSame(failure, assertThrows(OdpsException.class,
        () -> ServerCapabilities.namespaceSchemaEnabled(odps)));
  }

  @Test
  void unexpectedErrorsMustNotBePresentedAsMissingCapabilities() throws Exception {
    Odps odps = projectWithProperty("true");
    AssertionError failure = new AssertionError("probe programming defect");
    Project project = odps.projects().get("capability_test");
    doThrow(failure).when(project).reload();
    assertSame(failure, assertThrows(AssertionError.class,
        () -> ServerCapabilities.namespaceSchemaEnabled(odps)));
  }
}
