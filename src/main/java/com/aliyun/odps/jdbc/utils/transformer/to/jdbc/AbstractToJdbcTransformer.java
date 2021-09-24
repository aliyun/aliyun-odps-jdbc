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

import java.io.UnsupportedEncodingException;
import java.sql.SQLException;

import com.aliyun.odps.type.TypeInfo;


public abstract class AbstractToJdbcTransformer {
  static final String INVALID_TRANSFORMATION_ERROR_MSG =
      "Cannot transform ODPS-SDK Java class %s to %s";
  static final String ENCODING_ERR_MSG =
      "Error happened during encoding, please check the charset";
  static final String TRANSFORMATION_ERR_MSG =
      "Error happened when transforming %s into %s";

  /**
   * Transform ODPS SDK object to JDBC object
   * @param o java object from ODPS SDK
   * @param charset charset to encode byte array
   * @return JDBC object
   * @throws SQLException
   */
  public abstract Object transform(Object o, String charset) throws SQLException;

  public Object transform(Object o, String charset, TypeInfo odpsType) throws SQLException {
    // default implement
    return transform(o, charset);
  }

  static String getInvalidTransformationErrorMsg(Class odpsCls, Class jdbcCls) {
    return String.format(INVALID_TRANSFORMATION_ERROR_MSG, odpsCls.getName(), jdbcCls.getName());
  }

  static String getTransformationErrMsg(Object o, Class jdbcCls) {
    return String.format(TRANSFORMATION_ERR_MSG, o.toString(), jdbcCls.getName());
  }

  public static String encodeBytes(byte[] bytes, String charset) throws SQLException {
    if (charset != null) {
      try {
        return new String(bytes, charset);
      } catch (UnsupportedEncodingException e) {
        throw new SQLException(ENCODING_ERR_MSG, e);
      }
    }
    return new String(bytes);
  }
}