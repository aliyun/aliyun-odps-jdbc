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

public class Utils {

  public static String retrieveVersion() {
    // TODO: remove the hard code later
    // see http://stackoverflow.com/questions/3697449/retrieve-version-from-maven-pom-xml-in-code
    return "1.1";
  }

  public static boolean matchPattern(String s, String pattern) {

    if (pattern.contains("%") || pattern.contains("_")) {
      // (?<!a)  looks 1 char behind and ensure not equal
      String wildcard = pattern.replaceAll("(?<!\\\\)%", "\\\\w*").replaceAll("(?<!\\\\)_", "\\\\w");

      // escape / and %
      wildcard = wildcard.replace("\\%", "%").replace("\\_", "_");

      if (!s.matches(wildcard)) {
        return false;
      }
    } else {
      if (!s.equals(pattern)) {
        return false;
      }
    }

    return true;
  }
}
