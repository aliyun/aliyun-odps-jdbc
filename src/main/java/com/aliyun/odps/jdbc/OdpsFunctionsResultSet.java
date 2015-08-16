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
import java.sql.SQLException;
import java.util.Iterator;

import com.aliyun.odps.Function;

public class OdpsFunctionsResultSet extends OdpsResultSet implements ResultSet {

  private Iterator<Function> iterator;
  private Function function = null;

  OdpsFunctionsResultSet(Iterator<Function> iterator, OdpsResultSetMetaData meta)
      throws SQLException {
    super(null, meta);
    this.iterator = iterator;
  }

  @Override
  public void close() throws SQLException {
    isClosed = true;
    iterator = null;
    function = null;
    meta = null;
  }

  @Override
  public boolean next() throws SQLException {
    checkClosed();

    if (iterator.hasNext()) {
      function = iterator.next();
      return true;
    } else {
      function = null;
      return false;
    }
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    checkClosed();

    switch (columnIndex) {
      case 1: // FUNCTION_CAT
        return null;
      case 2: // FUNCTION_SCHEM
        return null;
      case 3: // FUNCTION_NAME
        return function.getName();
      case 4: // REMARKS
        return null;
      case 5: // FUNCTION_TYPE
        return (long) 0;   // TODO: set a more reasonable value
      case 6: // SPECIFIC_NAME
        return null;
      default:
        return null;
    }
  }
}
