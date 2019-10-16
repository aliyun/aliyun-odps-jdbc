package com.aliyun.odps.jdbc.utils.transformer.to.odps;

import com.aliyun.odps.OdpsType;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class ToOdpsTransformerFactory {

  private ToOdpsTransformerFactory() {
  }

  private static ToOdpsTinyintTransformer TINYINT_TRANSFORMER = new ToOdpsTinyintTransformer();
  private static ToOdpsSmallintTransformer SMALLINT_TRANSFORMER = new ToOdpsSmallintTransformer();
  private static ToOdpsIntTransformer INT_TRANSFORMER = new ToOdpsIntTransformer();
  private static ToOdpsBigIntTransformer BIGINT_TRANSFORMER = new ToOdpsBigIntTransformer();
  private static ToOdpsFloatTransformer FLOAT_TRANSFORMER = new ToOdpsFloatTransformer();
  private static ToOdpsDoubleTransformer DOUBLE_TRANSFORMER = new ToOdpsDoubleTransformer();
  private static ToOdpsDecimalTransformer DECIMAL_TRANSFORMER = new ToOdpsDecimalTransformer();
  private static ToOdpsVarcharTransformer VARCHAR_TRANSFORMER = new ToOdpsVarcharTransformer();
  private static ToOdpsStringTransformer STRING_TRANSFORMER = new ToOdpsStringTransformer();
  private static ToOdpsDatetimeTransformer DATETIME_TRANSFORMER = new ToOdpsDatetimeTransformer();
  private static ToOdpsTimeStampTransformer TIMESTAMP_TRANSFORMER = new ToOdpsTimeStampTransformer();
  private static ToOdpsBooleanTransformer BOOLEAN_TRANSFORMER = new ToOdpsBooleanTransformer();

  private static final Map<OdpsType, AbstractToOdpsTransformer> ODPS_TYPE_TO_TRANSFORMER =
      new HashMap<OdpsType, AbstractToOdpsTransformer>();
  static {
    ODPS_TYPE_TO_TRANSFORMER.put(OdpsType.TINYINT, TINYINT_TRANSFORMER);
    ODPS_TYPE_TO_TRANSFORMER.put(OdpsType.SMALLINT, SMALLINT_TRANSFORMER);
    ODPS_TYPE_TO_TRANSFORMER.put(OdpsType.INT, INT_TRANSFORMER);
    ODPS_TYPE_TO_TRANSFORMER.put(OdpsType.BIGINT, BIGINT_TRANSFORMER);
    ODPS_TYPE_TO_TRANSFORMER.put(OdpsType.FLOAT, FLOAT_TRANSFORMER);
    ODPS_TYPE_TO_TRANSFORMER.put(OdpsType.DOUBLE, DOUBLE_TRANSFORMER);
    ODPS_TYPE_TO_TRANSFORMER.put(OdpsType.DECIMAL, DECIMAL_TRANSFORMER);
    ODPS_TYPE_TO_TRANSFORMER.put(OdpsType.VARCHAR, VARCHAR_TRANSFORMER);
    ODPS_TYPE_TO_TRANSFORMER.put(OdpsType.STRING, STRING_TRANSFORMER);
    ODPS_TYPE_TO_TRANSFORMER.put(OdpsType.DATETIME, DATETIME_TRANSFORMER);
    ODPS_TYPE_TO_TRANSFORMER.put(OdpsType.TIMESTAMP, TIMESTAMP_TRANSFORMER);
    ODPS_TYPE_TO_TRANSFORMER.put(OdpsType.BOOLEAN, BOOLEAN_TRANSFORMER);
  }

  public static AbstractToOdpsTransformer getTransformer(OdpsType odpsType) throws SQLException {
    AbstractToOdpsTransformer transformer = ODPS_TYPE_TO_TRANSFORMER.get(odpsType);
    if (transformer == null) {
      throw new SQLException("Not supported ODPS type: " + odpsType);
    }
    return transformer;
  }
}
