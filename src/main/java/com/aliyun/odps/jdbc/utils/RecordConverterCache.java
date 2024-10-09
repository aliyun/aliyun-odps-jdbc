package com.aliyun.odps.jdbc.utils;

import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

import com.aliyun.odps.data.converter.OdpsRecordConverter;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class RecordConverterCache {

  static Map<String, ThreadLocal<OdpsRecordConverter>> recordConverters;

  public static OdpsRecordConverter get(TimeZone timeZone) {
    if (recordConverters == null) {
      recordConverters = new ConcurrentHashMap<>();
    }
    if (recordConverters.get(timeZone.getID()) == null) {
      recordConverters.put(timeZone.getID(), ThreadLocal.withInitial(() -> {
        return OdpsRecordConverter.builder()
            .setStrictMode(false)
            .timezone(timeZone.getID())
            .build();
      }));
    }
    return recordConverters.get(timeZone.getID()).get();
  }

  /**
   * clear all record converter, should be called when odps driver is de-registered
   * TODO: not called now
   */
  public static void cleanup() {
    for (ThreadLocal<OdpsRecordConverter> converter : recordConverters.values()) {
      converter.remove();
    }
    recordConverters.clear();
  }
}
