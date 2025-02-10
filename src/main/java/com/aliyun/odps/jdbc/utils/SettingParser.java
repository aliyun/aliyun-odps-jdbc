package com.aliyun.odps.jdbc.utils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SettingParser {
  public static ParseResult parse(String query) {
    SettingParser parser = new SettingParser();
    return parser.extractSetStatements(query);
  }

  enum State {
    DEFAULT,
    SINGLE_LINE_COMMENT,
    MULTI_LINE_COMMENT,
    IN_SET,
    IN_KEY_VALUE,
    STOP
  }

  public static class ParseResult {

    private final Map<String, String> settings;
    private final String remainingQuery;
    private final List<String> errors;

    public ParseResult(Map<String, String> settings, String remainingQuery, List<String> errors) {
      this.settings = settings;
      this.remainingQuery = remainingQuery;
      this.errors = errors;
    }

    public Map<String, String> getSettings() {
      return settings;
    }

    public String getRemainingQuery() {
      return remainingQuery;
    }

    public List<String> getErrors() {
      return errors;
    }
  }

  public ParseResult extractSetStatements(String s) {
    Map<String, String> settings = new LinkedHashMap<>();
    List<String> errors = new ArrayList<>();
    List<int[]> excludeRanges = new ArrayList<>();
    State currentState = State.DEFAULT;
    int i = 0;
    int currentStartIndex = -1;

    while (i < s.length()) {
      switch (currentState) {
        case DEFAULT:
          if (i <= s.length() - 2 && s.startsWith("--", i)) {
            currentState = State.SINGLE_LINE_COMMENT;
            i += 2;
          } else if (i <= s.length() - 2 && s.startsWith("/*", i)) {
            currentState = State.MULTI_LINE_COMMENT;
            i += 2;
          } else if (i <= s.length() - 3 && (s.startsWith("set", i) || s.startsWith("SET", i))) {
            if (i + 3 < s.length() && Character.isWhitespace(s.charAt(i + 3))) {
              currentState = State.IN_SET;
              currentStartIndex = i;
              i += 4; // Skip 'set' and the following whitespace
            } else {
              i++;
            }
          } else {
            if (!Character.isWhitespace(s.charAt(i))) {
              currentState = State.STOP;
            }
            i++;
          }
          break;
        case SINGLE_LINE_COMMENT:
          while (i < s.length() && s.charAt(i) != '\n') {
            i++;
          }
          if (i < s.length()) {
            i++;
          }
          currentState = State.DEFAULT;
          break;
        case MULTI_LINE_COMMENT:
          while (i < s.length()) {
            if (i + 1 < s.length() && s.startsWith("*/", i)) {
              i += 2;
              currentState = State.DEFAULT;
              break;
            } else {
              i++;
            }
          }
          break;
        case IN_SET:
          while (i < s.length() && Character.isWhitespace(s.charAt(i))) {
            i++;
          }
          if (i < s.length()) {
            currentState = State.IN_KEY_VALUE;
          } else {
            errors.add("Invalid SET statement: missing key-value after 'set'");
            currentStartIndex = -1;
            currentState = State.DEFAULT;
          }
          break;
        case IN_KEY_VALUE:
          int keyValueStart = i;
          boolean foundSemicolon = false;
          while (i < s.length()) {
            if (s.charAt(i) == ';' && s.charAt(i - 1) != '\\') {
              foundSemicolon = true;
              i++;
              break;
            }
            i++;
          }
          if (foundSemicolon) {
            excludeRanges.add(new int[]{currentStartIndex, i});
            String kv = s.substring(keyValueStart, i - 1).trim();
            parseKeyValue(kv, settings, errors);
          } else {
            errors.add("Invalid SET statement: missing semicolon");
          }
          currentState = State.DEFAULT;
          currentStartIndex = -1;
          break;
        case STOP:
          i = s.length();
          break;
        default:
      }
    }

    // Build remaining query
    StringBuilder remainingQuery = new StringBuilder();
    int currentPos = 0;
    for (int[] range : excludeRanges) {
      if (currentPos < range[0]) {
        remainingQuery.append(s, currentPos, range[0]);
      }
      currentPos = range[1];
    }
    if (currentPos < s.length()) {
      remainingQuery.append(s, currentPos, s.length());
    }

    return new ParseResult(settings, remainingQuery.toString(), errors);
  }

  private void parseKeyValue(String kv, Map<String, String> settings, List<String> errors) {
    int eqIdx = kv.indexOf('=');
    if (eqIdx == -1) {
      errors.add("Invalid key-value pair '" + kv + "': missing '='");
      return;
    }
    String key = kv.substring(0, eqIdx).trim();
    if (key.isEmpty()) {
      errors.add("Invalid key-value pair '" + kv + "': empty key");
      return;
    }
    String value = eqIdx < kv.length() - 1 ? kv.substring(eqIdx + 1).trim() : "";
    value = value.replace("\\;", ";");
    settings.put(key, value);
  }
}
