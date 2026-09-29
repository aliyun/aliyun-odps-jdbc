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
import com.aliyun.odps.Project;

/**
 * Capabilities of the service that a test case can need and the test project may not provide.
 *
 * <p>Some statements are rejected for reasons that have nothing to do with the driver. {@code show
 * schemas} and any {@code project.schema.object} reference, for instance, need the project to run
 * in the three-tier model. Reported as ordinary errors they are indistinguishable from a driver
 * regression, so a case that needs such a capability says so up front and is skipped with the
 * reason and the way to turn the capability on.
 *
 * <p>Capabilities a statement can enable for itself -- the odps2 type system, decimal2 -- are the
 * other kind and are <b>not</b> skipped here: the case sets the session flag, as
 * {@code OdpsPreparedStatementTest} (JSON table), {@code OdpsJdbcDateTimeTest} (DATE/TIMESTAMP) and
 * {@code time.ServerSideTest} already do. A project-level capability cannot be bought that way:
 * measured on a two-tier project, {@code set odps.namespace.schema=true} leaves {@code show
 * schemas} failing with ODPS-0110061 and three-part DDL failing with ODPS-0130161.
 */
public final class ServerCapabilities {

  /**
   * Project property that reports whether the three-tier model is on.
   *
   * <p>The property, and not the error code, is the predicate: on a two-tier project the same
   * missing capability surfaces as ODPS-0110061 for {@code show schemas} and as ODPS-0130161 for a
   * {@code project.schema.object} name, so a gate keyed on whichever statement happened to run
   * first would miss the other.
   */
  public static final String NAMESPACE_SCHEMA_PROPERTY = "odps.schema.model.enabled";

  private ServerCapabilities() {
  }

  /**
   * @return whether the default project of {@code odps} runs in the three-tier model. A project
   *         whose property cannot be read counts as not enabled, so the caller skips with a reason
   *         instead of failing on a statement the service would reject anyway.
   */
  public static boolean namespaceSchemaEnabled(Odps odps) {
    try {
      Project project = odps.projects().get(odps.getDefaultProject());
      project.reload();
      String value = project.getProperty(NAMESPACE_SCHEMA_PROPERTY);
      if (value != null) {
        return Boolean.parseBoolean(value.trim());
      }
      // Property absent on this service build: fall back to the schema list, which the service
      // answers only for three-tier projects.
      return odps.schemas().exists("default");
    } catch (Throwable t) {
      return false;
    }
  }

  /**
   * Condition method for {@code @DisabledIf}, addressed as
   * "com.aliyun.odps.jdbc.utils.ServerCapabilities#namespaceSchemaDisabled".
   *
   * <p>A class-level condition rather than {@code Assumptions.assumeTrue(..)} inside
   * {@code @BeforeAll}, deliberately: an aborted container is reported by surefire as
   * {@code tests="0" skipped="0"}, which is a class silently executing nothing -- the exact shape
   * pinning the surefire version was meant to make loud. This way every affected case stays visible
   * as skipped, with the reason.
   */
  public static boolean namespaceSchemaDisabled() {
    Odps odps;
    try {
      odps = TestUtils.getOdps();
    } catch (Throwable t) {
      return true; // no test identity at all: nothing here can run, and the reason below says so
    }
    return !namespaceSchemaEnabled(odps);
  }
}
