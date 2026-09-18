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

import java.sql.ResultSet;
import java.sql.Types;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class MetadataOfflineContractTest {
  @Test void exportedKeysReturnsEmptyStandardMetadata() throws Exception {
    OdpsDatabaseMetaData metadata = new OdpsDatabaseMetaData(mock(OdpsConnection.class));
    try (ResultSet rows = metadata.getExportedKeys(null, null, "table")) {
      assertFalse(rows.next());
      assertEquals(14, rows.getMetaData().getColumnCount());
      assertEquals("PKTABLE_CAT", rows.getMetaData().getColumnName(1));
      assertEquals("DEFERRABILITY", rows.getMetaData().getColumnName(14));
      for (int index : new int[]{9, 10, 11, 14}) {
        assertEquals(Types.SMALLINT, rows.getMetaData().getColumnType(index));
      }
    }
  }
}
