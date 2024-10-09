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

package com.aliyun.odps.jdbc.utils.transformer.to.jdbc;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Objects;

import com.aliyun.odps.data.AbstractChar;
import com.aliyun.odps.data.Binary;


public class ToJdbcBigDecimalTransformer extends AbstractToJdbcTransformer {

  @Override
  public Object transform(Object o, String charset) throws SQLException {
    if (o == null) {
      return null;
    }

    try {
      if (o instanceof BigDecimal) {
        return o;
      } else if (o instanceof Number || o instanceof String || o instanceof AbstractChar) {
        return new BigDecimal(o.toString().trim());
      } else if (o instanceof Binary) {
        String str = encodeBytes(((Binary) o).data(), charset);
        return new BigDecimal(str);
      } else if (o instanceof byte[]) {
        String str = encodeBytes((byte[]) o, charset);
        return new BigDecimal(str);
      } else {
        String errorMsg = getInvalidTransformationErrorMsg(o.getClass(), BigDecimal.class);
        throw new SQLException(errorMsg);
      }
    } catch (SQLException e) {
      throw e;
    } catch (Exception e) {
      String
          errorMsg =
          getTransformationErrMsg(Objects.toString(o), BigDecimal.class, e.getMessage());
      throw new SQLException(errorMsg, e);
    }
  }
}
