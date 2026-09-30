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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.jdbc.utils.JdbcColumn;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;

/**
 * Offline contract for the size and scale a column reports.
 *
 * <p>{@code DatabaseMetaData.getColumns()} and {@code ResultSetMetaData} describe the same column
 * of the same table, so the two views must publish the same precision and the same number of
 * fractional digits. Before this contract was pinned down, {@code getColumns()} reported
 * COLUMN_SIZE null and DECIMAL_DIGITS 0 for every type while {@code getPrecision()}/
 * {@code getScale()} reported the declared values, so a consumer sizing a column out of the
 * catalog (a widget width, a cast, a "will this fit" check) had nothing to work with.
 *
 * <p>No service round trip: both views are built from one column list.
 */
public class MetadataColumnSizeScaleTest {

  private static final List<Column> COLUMNS = Arrays.asList(
      new Column("id", TypeInfoFactory.BIGINT),
      new Column("i", TypeInfoFactory.INT),
      new Column("si", TypeInfoFactory.SMALLINT),
      new Column("ti", TypeInfoFactory.TINYINT),
      new Column("dbl", TypeInfoFactory.DOUBLE),
      new Column("f", TypeInfoFactory.FLOAT),
      new Column("dec", TypeInfoFactory.getDecimalTypeInfo(10, 2)),
      new Column("ch", TypeInfoFactory.getCharTypeInfo(5)),
      new Column("vc", TypeInfoFactory.getVarcharTypeInfo(20)),
      new Column("s", TypeInfoFactory.STRING),
      new Column("ts", TypeInfoFactory.TIMESTAMP),
      new Column("dt", TypeInfoFactory.DATETIME),
      new Column("d", TypeInfoFactory.DATE),
      new Column("bl", TypeInfoFactory.BOOLEAN),
      new Column("bin", TypeInfoFactory.BINARY),
      new Column("arr", TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.BIGINT)),
      new Column("mp", TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.STRING,
                                                       TypeInfoFactory.BIGINT)));

  private static List<String> names() {
    List<String> out = new ArrayList<>();
    for (Column c : COLUMNS) {
      out.add(c.getName());
    }
    return out;
  }

  private static List<TypeInfo> typeInfos() {
    List<TypeInfo> out = new ArrayList<>();
    for (Column c : COLUMNS) {
      out.add(c.getTypeInfo());
    }
    return out;
  }

  private static JdbcColumn wrap(Column c, int ordinal) {
    return new JdbcColumn(c.getName(), "t", "sch", c.getTypeInfo().getOdpsType(),
                          c.getTypeInfo(), c.getComment(), ordinal);
  }

  @Test
  public void columnSizeIsTheDeclaredPrecisionAndStaysNullWhenUnbounded() throws Exception {
    for (int i = 0; i < COLUMNS.size(); i++) {
      Column c = COLUMNS.get(i);
      Integer size = wrap(c, i + 1).getColumnSize();
      int declared = JdbcColumn.columnPrecision(c.getTypeInfo());
      String what = c.getName() + " (" + c.getTypeInfo().getTypeName() + ")";
      if (declared == Integer.MAX_VALUE) {
        Assertions.assertNull(size, what + ": MaxCompute does not bound this type, so the "
            + "size stays null instead of claiming 2^31-1");
      } else {
        Assertions.assertEquals(Integer.valueOf(declared), size, what + ": COLUMN_SIZE");
      }
    }
  }

  @Test
  public void decimalDigitsIsTheScaleResultSetMetaDataReports() throws Exception {
    OdpsResultSetMetaData rsmd = new OdpsResultSetMetaData(names(), typeInfos());
    for (int i = 0; i < COLUMNS.size(); i++) {
      Column c = COLUMNS.get(i);
      String what = c.getName() + " (" + c.getTypeInfo().getTypeName() + ")";
      Assertions.assertEquals(rsmd.getScale(i + 1), wrap(c, i + 1).getDecimalDigits(),
          what + ": DECIMAL_DIGITS must equal ResultSetMetaData.getScale()");
      Integer size = wrap(c, i + 1).getColumnSize();
      if (size != null) {
        Assertions.assertEquals(rsmd.getPrecision(i + 1), size.intValue(),
            what + ": COLUMN_SIZE must equal ResultSetMetaData.getPrecision()");
      } else {
        Assertions.assertEquals(Integer.MAX_VALUE, rsmd.getPrecision(i + 1),
            what + ": no size is reported only where the precision is unbounded");
      }
    }
  }

  @Test
  public void theDeclaredScaleOfADecimalColumnIsTheOneInTheType() throws Exception {
    Column dec = new Column("dec", TypeInfoFactory.getDecimalTypeInfo(10, 2));
    JdbcColumn wrapped = wrap(dec, 1);
    Assertions.assertEquals(Integer.valueOf(10), wrapped.getColumnSize(),
        "DECIMAL(10,2) is 10 digits wide");
    Assertions.assertEquals(2, wrapped.getDecimalDigits(),
        "DECIMAL(10,2) has 2 fractional digits");

    Column ch = new Column("ch", TypeInfoFactory.getCharTypeInfo(5));
    Assertions.assertEquals(Integer.valueOf(5), wrap(ch, 1).getColumnSize());
    Assertions.assertEquals(0, wrap(ch, 1).getDecimalDigits());

    Column ts = new Column("ts", TypeInfoFactory.TIMESTAMP);
    Assertions.assertEquals(9, wrap(ts, 1).getDecimalDigits(),
        "TIMESTAMP keeps nanoseconds, and getScale() already says 9");
  }
}
