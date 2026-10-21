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

package com.aliyun.odps.jdbc.utils;

import org.junit.jupiter.api.Test;
import java.util.Locale;
import static org.junit.jupiter.api.Assertions.*;

class MetadataPatternContractTest {
  @Test void jdbcWildcardsMatchUnicodeAndPunctuation() {
    assertTrue(Utils.matchPattern("表-1", "%"));
    assertTrue(Utils.matchPattern("表-1", "__1"));
    assertTrue(Utils.matchPattern("a.b", "a.b"));
    assertFalse(Utils.matchPattern("axb", "a.b"));
    assertTrue(Utils.matchPattern("a[b", "a[b"));
    assertTrue(Utils.matchPattern("a%b", "a\\%b"));
    assertTrue(Utils.matchPattern("a_b", "a\\_b"));
    assertTrue(Utils.matchPattern("a\\b", "a\\\\b"));
    assertTrue(Utils.matchPattern("anything", null));
    assertFalse(Utils.matchPattern("anything", ""));
    assertTrue(Utils.matchPattern("", ""));
  }
  @Test void matchingDoesNotDependOnDefaultLocale() {
    Locale original = Locale.getDefault();
    try {
      Locale.setDefault(new Locale("tr", "TR"));
      assertTrue(Utils.matchPattern("IDENTIFIER", "identifier"));
    } finally { Locale.setDefault(original); }
  }
}
