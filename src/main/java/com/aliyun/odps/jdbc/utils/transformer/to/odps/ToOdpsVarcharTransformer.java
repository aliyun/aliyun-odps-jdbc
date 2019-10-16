package com.aliyun.odps.jdbc.utils.transformer.to.odps;

import com.aliyun.odps.data.Varchar;
import java.sql.SQLException;

public class ToOdpsVarcharTransformer extends AbstractToOdpsTransformer {

  @Override
  public Object transform(Object o, String charset) throws SQLException {
    if (o == null) {
      return null;
    }

    if (String.class.isInstance(o)) {
      return new Varchar((String) o);
    } else if (byte[].class.isInstance(o)) {
      return new Varchar(encodeBytes((byte[]) o, charset));
    } else {
      String errorMsg = getInvalidTransformationErrorMsg(o.getClass(), Varchar.class);
      throw new SQLException(errorMsg);
    }
  }
}
