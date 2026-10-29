package com.aliyun.odps.jdbc.utils;

import com.aliyun.odps.commons.GeneralConfiguration;

/**
 * Applies the two connection-level Tunnel timeouts to a Tunnel client created from a connection.
 *
 * <p>{@code tunnelConnectTimeout} and {@code tunnelReadTimeout} are seconds and are copied over
 * without rescaling. The gate is the one the driver has always used on the result-download path: a
 * negative value means "not configured", so the SDK default (180 s connect, 300 s read) stays in
 * place, while {@code 0} is written on purpose and removes the SDK-imposed bound. See the
 * "Units, and what -1 really means" note in README for the measured readings.
 */
public final class TunnelTimeouts {

  private TunnelTimeouts() {
  }

  /** Writes each option only when it is configured, leaving the other one at the SDK default. */
  public static void apply(GeneralConfiguration config, int connectTimeoutSeconds,
                           int readTimeoutSeconds) {
    if (connectTimeoutSeconds >= 0) {
      config.setSocketConnectTimeout(connectTimeoutSeconds);
    }
    if (readTimeoutSeconds >= 0) {
      config.setSocketTimeout(readTimeoutSeconds);
    }
  }
}
