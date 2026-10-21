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

import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import com.aliyun.odps.jdbc.utils.transformer.to.jdbc.ToJdbcStringTransformer;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.gson.JsonParser;

class ComplexStringContractTest {
  @Test void mapUsesJsonInsteadOfJavaMapToString() throws Exception {
    Map<String, String> value = new LinkedHashMap<>();
    value.put("2", "b"); value.put("1", "a");
    Object text = new ToJdbcStringTransformer().transform(value, "UTF-8", null,
        TimeZone.getTimeZone("UTC"), TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.STRING, TypeInfoFactory.STRING));
    assertEquals("b", new JsonParser().parse((String) text).getAsJsonObject().get("2").getAsString());
    assertEquals("a", new JsonParser().parse((String) text).getAsJsonObject().get("1").getAsString());
  }
  @Test void arrayPreservesSpecialCharacters() throws Exception {
    Object text = new ToJdbcStringTransformer().transform(Arrays.asList("<>&=", "中文"), "UTF-8", null,
        TimeZone.getTimeZone("UTC"), TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.STRING));
    assertEquals("<>&=", new JsonParser().parse((String) text).getAsJsonArray().get(0).getAsString());
    assertEquals("中文", new JsonParser().parse((String) text).getAsJsonArray().get(1).getAsString());
  }
}
