package com.aliyun.odps.jdbc.utils.transformer.to.odps;

import java.io.UnsupportedEncodingException;
import java.sql.SQLException;

public abstract class AbstractToOdpsTransformer {

  static final String INVALID_TRANSFORMATION_ERROR_MSG =
      "Cannot transform JDBC java class %s to %s";
  static final String ENCODING_ERR_MSG =
      "Error happened during encoding, please check the charset";
  static final String TRANSFORMATION_ERR_MSG =
      "Error happened when transforming %s into %s";

  /**
   * Transform JDBC object to ODPS SDK object
   * @param o JDBC object
   * @return ODPS SDK object
   * @throws SQLException
   */
  public abstract Object transform(Object o, String charset) throws SQLException;

  static String getInvalidTransformationErrorMsg(Class jdbcCls, Class odpsCls) {
    return String.format(INVALID_TRANSFORMATION_ERROR_MSG, jdbcCls.getName(), odpsCls.getName());
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
