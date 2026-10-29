package com.aliyun.odps.jdbc.utils;

import java.net.URI;
import java.net.URISyntaxException;

/** Normalizes and validates the explicit connection Tunnel endpoint. */
public final class TunnelEndpoint {
  private TunnelEndpoint() {
  }

  public static String normalize(String value) {
    if (value == null || value.trim().isEmpty()) {
      return null;
    }
    String endpoint = value.trim();
    try {
      URI uri = new URI(endpoint);
      if (("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme()))
          && uri.getHost() != null && uri.getRawUserInfo() == null
          && uri.getRawQuery() == null && uri.getRawFragment() == null) {
        return endpoint;
      }
    } catch (URISyntaxException ignored) {
      // Report the option without echoing a possibly credential-bearing URL.
    }
    throw new IllegalArgumentException(
        "Invalid tunnelEndpoint (tunnel_endpoint): expected an absolute HTTP(S) base URL"
        + " without user information, query or fragment");
  }
}
