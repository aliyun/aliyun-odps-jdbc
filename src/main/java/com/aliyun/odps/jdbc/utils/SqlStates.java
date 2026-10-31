/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.jdbc.utils;

import com.aliyun.odps.NoSuchObjectException;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.rest.ErrorMessage;
import com.aliyun.odps.rest.RestException;

import java.sql.SQLException;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The standard error identity of a failed JDBC call.
 *
 * <p>{@link SQLException#getSQLState()} and {@link SQLException#getErrorCode()} exist so that a
 * caller which has never seen this driver can branch on what went wrong: a connection pool choosing
 * between retry, back-off and fail-fast; a BI or ETL tool mapping a failure into its own message;
 * an application that must tell "the table is gone" from "the credentials were refused". Until now
 * the driver reported every service failure with {@code null} and {@code 0}, and the only readable
 * identity was the {@code ODPS-0130071}-style code inside the message text -- see
 * <a href="https://github.com/aliyun/aliyun-odps-jdbc/issues/67">issue #67</a>.
 *
 * <h2>What this class will and will not say</h2>
 *
 * <ul>
 *   <li>The states are the published SQL-standard / JDBC class values (08 connection, 28
 *       authorization, 3D catalog, 3F schema, 42 syntax and access rule, 0A feature, HYT timeout,
 *       55 object state). Nothing here invents a code, and nothing here pretends to be another
 *       database's table.
 *   <li>A state is reported only where the driver has evidence for it. When the failure carries no
 *       recognizable service code -- a {@code NullPointerException}, or an {@code ODPS-99xxxxx} the
 *       driver has never seen -- {@code getSQLState()} stays {@code null}. A fabricated
 *       {@code HY000} teaches callers to branch on a value that means nothing.
 *   <li>{@code getErrorCode()} carries the numeric part of the MaxCompute service code whenever the
 *       service gave one ({@code ODPS-0130071} becomes {@code 130071}), even when the state cannot
 *       be classified. {@code 0} still means "no information", which is what it meant before.
 *   <li>Message text, exception type and cause chain are not changed. Whatever worked by reading the
 *       message keeps working; these two fields were empty before.
 * </ul>
 *
 * <h2>Where the evidence comes from</h2>
 *
 * <p>Three public signals, searched through the whole cause chain because the SDK wraps: the service
 * error code name in {@link OdpsException#getErrorCode()} (filled from the REST {@code <Code>}
 * element, e.g. {@code NoSuchObject}, {@code Unauthorized}, {@code NoPermission}), the numeric
 * {@code ODPS-<digits>} in any message on that chain (the SQL compiler, metadata and MaxQA session
 * families), and the typed {@link NoSuchObjectException}. Transport failures ({@code IOException}
 * and friends) carry no service identity at all, so they are classified by the call site that caught
 * them: an opening failure that never reached a service is {@code 08001}, a failure while streaming
 * a result is {@code 08S01}.
 */
public final class SqlStates {

  /** The client could not establish a connection: unreachable endpoint, refused option value. */
  public static final String UNABLE_TO_CONNECT = "08001";

  /** The service answered the opening attempt and refused it, for a reason we cannot name better. */
  public static final String CONNECTION_REJECTED = "08004";

  /** The caller asked for work on a connection it has already closed. */
  public static final String CONNECTION_NOT_IN_USE = "08003";

  /** A connection that was open failed while it was being used: read, fetch, commit. */
  public static final String COMMUNICATION_LINK_FAILURE = "08S01";

  /** The identity the caller presented is not accepted: bad signature, invalid or expired token. */
  public static final String INVALID_AUTHORIZATION = "28000";

  /** The project named by the caller does not exist (in this driver, catalog == project). */
  public static final String INVALID_CATALOG_NAME = "3D000";

  /** The schema named by the caller does not exist, or the project has no schema layer at all. */
  public static final String INVALID_SCHEMA_NAME = "3F000";

  /** The table named by the caller does not exist. */
  public static final String TABLE_NOT_FOUND = "42S02";

  /** The statement was rejected by the compiler: syntax, resolution, semantic analysis. */
  public static final String SYNTAX_ERROR_OR_ACCESS_RULE = "42000";

  /** The identity was accepted, but not for this object: authorization or session access denied. */
  public static final String INSUFFICIENT_PRIVILEGE = "42501";

  /** The service or the driver does not implement what the caller asked for. */
  public static final String FEATURE_NOT_SUPPORTED = "0A000";

  /** The query outlived the time the caller allowed it. */
  public static final String QUERY_TIMEOUT_EXPIRED = "HYT00";

  /** The object the caller used is not in the state that use requires -- it has been closed. */
  public static final String OBJECT_NOT_IN_STATE = "55000";

  /**
   * A service code classified differently from its family: a two-tier project has no schema layer,
   * so asking it for one is a schema-name problem rather than a compile error.
   */
  static final int CODE_TWO_TIER_MODEL_REJECTS_SCHEMA_OPERATION = 110061;

  // The first three digits of the service code, as published: "013" is the SQL compiler family,
  // "182"/"185"/"186" are the MaxQA session families (SQLExecutorConstants in the SDK names them).
  private static final String FAMILY_SQL_COMPILER = "013";
  private static final String FAMILY_SESSION_ACCESS_DENIED = "182";
  private static final String FAMILY_SESSION_UNSUPPORTED = "185";
  private static final String FAMILY_SESSION_TIMEOUT = "186";

  private static final int CODE_AUTHORIZATION = 130013;
  private static final int CODE_TABLE_MISSING = 130131;
  private static final int CODE_PROJECT_OR_SCHEMA_MISSING = 420111;

  private static final Pattern SERVICE_CODE = Pattern.compile("ODPS-(\\d{6,8})");

  /** Bound the walk: a cause chain is expected to be shallow, and a cycle must not spin. */
  private static final int MAX_CAUSE_DEPTH = 32;

  private SqlStates() {
  }

  /**
   * @return the SQLState for a failure, or {@code null} when the driver has no evidence for one
   */
  public static String resolveSqlState(Throwable failure) {
    return resolveSqlState(failure, null);
  }

  /**
   * The state for a failure, as seen from a call site that knows something the failure does not.
   *
   * @param stateWithoutEvidence what to report when nothing in the chain identifies the failure --
   *     pass {@code null} to leave {@code getSQLState()} empty rather than guess
   */
  public static String resolveSqlState(Throwable failure, String stateWithoutEvidence) {
    String state = collect(failure).classify();
    return state != null ? state : stateWithoutEvidence;
  }

  /**
   * @return the numeric MaxCompute service code carried anywhere in the chain, or {@code 0}
   */
  public static int resolveErrorCode(Throwable failure) {
    return collect(failure).serviceCode;
  }

  /**
   * Report a failure from a call site that adds no context: a state appears only when the chain
   * identifies the failure itself.
   */
  public static SQLException sqlException(String message, Throwable cause) {
    return sqlException(message, cause, null);
  }

  /** Report a failure, with the state this call site knows it has to be if nothing else does. */
  public static SQLException sqlException(String message, Throwable cause,
                                          String stateWithoutEvidence) {
    Evidence evidence = collect(cause);
    String state = evidence.classify();
    if (state == null) {
      state = stateWithoutEvidence;
    }
    return build(message, state, evidence.serviceCode, cause);
  }

  /**
   * Report a failure that happened while opening a connection.
   *
   * <p>Everything that keeps a caller from getting a connection is class 08 unless the chain says
   * more: the service refused us ({@code 08004}), or its answer names the problem ({@code 28000} for
   * an identity it will not accept, {@code 3D000} for a project that is not there, and so on). A
   * mistyped option value and an unreachable endpoint both end up as {@code 08001}: from the
   * caller's side they are the same answer -- this URL does not open a connection.
   */
  public static SQLException connectFailure(String message, Throwable cause) {
    Evidence evidence = collect(cause);
    String state = evidence.classify();
    if (state == null) {
      state = evidence.reachedService ? CONNECTION_REJECTED : UNABLE_TO_CONNECT;
    }
    return build(message, state, evidence.serviceCode, cause);
  }

  private static SQLException build(String message, String state, int code, Throwable cause) {
    // A null state and a 0 code are exactly what a caller got before, so filling either field is
    // the only observable change this makes.
    return new SQLException(message, state, code, cause);
  }

  /** Everything one failure says, gathered before any rule looks at it. */
  private static final class Evidence {

    private int serviceCode;
    private String serviceFamily;
    private String codeName;
    private String text;
    private boolean reachedService;
    private boolean missingObjectTyped;

    private String classify() {
      if (serviceCode != 0) {
        if (serviceCode == CODE_AUTHORIZATION) {
          return INSUFFICIENT_PRIVILEGE;
        }
        if (serviceCode == CODE_TABLE_MISSING) {
          return TABLE_NOT_FOUND;
        }
        if (serviceCode == CODE_PROJECT_OR_SCHEMA_MISSING) {
          // The service uses this one code for both layers, so its own words decide.
          return mentionsSchema(text) && !mentionsProject(text)
                 ? INVALID_SCHEMA_NAME : INVALID_CATALOG_NAME;
        }
        if (serviceCode == CODE_TWO_TIER_MODEL_REJECTS_SCHEMA_OPERATION) {
          return INVALID_SCHEMA_NAME;
        }
        if (FAMILY_SESSION_UNSUPPORTED.equals(serviceFamily)) {
          return FEATURE_NOT_SUPPORTED;
        }
        if (FAMILY_SESSION_TIMEOUT.equals(serviceFamily)) {
          return QUERY_TIMEOUT_EXPIRED;
        }
        if (FAMILY_SESSION_ACCESS_DENIED.equals(serviceFamily)) {
          return INSUFFICIENT_PRIVILEGE;
        }
        if (isCredentialName(codeName) || isCredentialText(text)) {
          return INVALID_AUTHORIZATION;
        }
        if (FAMILY_SQL_COMPILER.equals(serviceFamily)) {
          return SYNTAX_ERROR_OR_ACCESS_RULE;
        }
        // A code from a family this driver has not classified: report the code, invent nothing.
        return null;
      }
      if (isCredentialName(codeName) || isCredentialText(text)) {
        return INVALID_AUTHORIZATION;
      }
      if (isPrivilegeName(codeName) || mentionsPrivilege(text)) {
        return INSUFFICIENT_PRIVILEGE;
      }
      if ("NoSuchSchema".equals(codeName)) {
        return INVALID_SCHEMA_NAME;
      }
      if (missingObjectTyped || "NoSuchObject".equals(codeName)
          || "NoSuchProject".equals(codeName)) {
        if (mentionsProject(text)) {
          return INVALID_CATALOG_NAME;
        }
        if (mentionsSchema(text)) {
          return INVALID_SCHEMA_NAME;
        }
        // A missing table is the only object the SDK reports with NoSuchObjectException.
        return TABLE_NOT_FOUND;
      }
      return null;
    }
  }

  private static Evidence collect(Throwable failure) {
    Evidence evidence = new Evidence();
    Throwable current = failure;
    for (int depth = 0; depth < MAX_CAUSE_DEPTH && current != null; depth++) {
      Throwable cause = current.getCause();
      if (cause == current) {
        break;
      }
      String message = current.getMessage();
      if (message != null && !message.isEmpty()) {
        evidence.text = evidence.text == null ? message : evidence.text + "\n" + message;
      }
      if (current instanceof NoSuchObjectException) {
        evidence.missingObjectTyped = true;
      }
      if (current instanceof RestException) {
        ErrorMessage errorMessage = ((RestException) current).getErrorMessage();
        if (errorMessage != null) {
          evidence.reachedService = true;
          adoptCodeName(evidence, errorMessage.getErrorcode());
          adoptServiceCode(evidence, errorMessage.getMessage());
        }
      }
      if (current instanceof OdpsException) {
        OdpsException odps = (OdpsException) current;
        if (odps.getStatus() != null) {
          evidence.reachedService = true;
        }
        // "Local Error" is what the tunnel SDK stamps on a failure it produced itself; it is not
        // an answer from the service, and reading it as one would classify a local problem as a
        // remote one.
        if (!"Local Error".equals(odps.getErrorCode())) {
          adoptCodeName(evidence, odps.getErrorCode());
        }
      }
      adoptServiceCode(evidence, message);
      current = cause;
    }
    return evidence;
  }

  /** The code name the service sent, if any; the first one on the chain is the reported failure. */
  private static void adoptCodeName(Evidence evidence, String codeName) {
    if (evidence.codeName == null && codeName != null && !codeName.isEmpty()) {
      evidence.codeName = codeName;
    }
  }

  /** The first service code in the chain wins: it is the one the failure was announced with. */
  private static void adoptServiceCode(Evidence evidence, String text) {
    if (evidence.serviceCode != 0 || text == null || text.isEmpty()) {
      return;
    }
    Matcher matcher = SERVICE_CODE.matcher(text);
    if (!matcher.find()) {
      return;
    }
    String digits = matcher.group(1);
    long parsed;
    try {
      parsed = Long.parseLong(digits);
    } catch (NumberFormatException overflow) {
      return;
    }
    if (parsed > Integer.MAX_VALUE) {
      return;
    }
    evidence.serviceCode = (int) parsed;
    evidence.serviceFamily = digits.substring(0, Math.min(3, digits.length()));
  }

  private static boolean mentionsProject(String text) {
    return contains(text, "project not found") || contains(text, "project '")
           || contains(text, "project does not exist");
  }

  private static boolean mentionsSchema(String text) {
    return contains(text, "schema not found") || contains(text, "schema '")
           || contains(text, "database operations");
  }

  private static boolean mentionsPrivilege(String text) {
    return contains(text, "authorization exception") || contains(text, "authorization failed")
           || contains(text, "no privilege") || contains(text, "access denied");
  }

  private static boolean isCredentialName(String codeName) {
    return "Unauthorized".equals(codeName) || "InvalidAccount".equals(codeName)
           || "SignatureDoesNotMatch".equals(codeName) || "InvalidSecurityToken".equals(codeName);
  }

  private static boolean isPrivilegeName(String codeName) {
    return "NoPermission".equals(codeName) || "AccessDenied".equals(codeName)
           || "Forbidden".equals(codeName);
  }

  private static boolean isCredentialText(String text) {
    return contains(text, "invalid signature") || contains(text, "signature does not match")
           || contains(text, "security token");
  }

  private static boolean contains(String text, String needle) {
    return text != null && text.toLowerCase(Locale.ROOT).contains(needle);
  }
}
