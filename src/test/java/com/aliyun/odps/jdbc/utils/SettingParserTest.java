package com.aliyun.odps.jdbc.utils;


import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import java.util.List;

import org.junit.jupiter.api.Test;

public class SettingParserTest {
  // 基础测试用例
  @Test
  public void testStandardScenario() {
    String sql = "/* pre comment */\n"
                 + "SET key1 = value1; -- inline comment\n"
                 + "SeT key2=value2;\n"
                 + "SELECT 1;";

    SettingParser parser = new SettingParser();
    SettingParser.ParseResult result = parser.extractSetStatements(sql);

    // 验证设置项
    Map<String, String> settings = result.getSettings();
    assertEquals(2, settings.size());
    assertEquals("value1", settings.get("key1"));
    assertEquals("value2", settings.get("key2"));

    // 验证剩余SQL
    String expectedRemaining = "/* pre comment */\n -- inline comment\n\nSELECT 1;";
    assertEquals(expectedRemaining, result.getRemainingQuery());

    // 验证错误信息
    assertTrue(result.getErrors().isEmpty());
  }

  // 注释嵌套测试
  @Test
  public void testNestedComments() {
    String sql = "/**//*! SET invalid1=1 */\n"
                 + "-- SET invalid2=2\n"
                 + "SET /*inner*/valid3=3; -- end\n"
                 + "SELECT '--string'/*, \"SET\"*/;";

    SettingParser.ParseResult result = new SettingParser().extractSetStatements(sql);

    assertEquals(1, result.getSettings().size());
    assertEquals("3", result.getSettings().get("/*inner*/valid3"));

    String expected = "/**//*! SET invalid1=1 */\n-- SET invalid2=2\n -- end\nSELECT '--string'/*, \"SET\"*/;";
    assertEquals(expected, result.getRemainingQuery());
  }

  // 错误场景测试
  @Test
  public void testErrorScenarios() {
    SettingParser parser = new SettingParser();

    // 缺少分号
    String sql1 = "SET key=value";
    SettingParser.ParseResult r1 = parser.extractSetStatements(sql1);
    assertEquals(1, r1.getErrors().size());
    assertTrue(r1.getErrors().get(0).contains("missing semicolon"));

    // 无效键值对
    String sql2 = "SET invalid;";
    SettingParser.ParseResult r2 = parser.extractSetStatements(sql2);
    assertEquals(1, r2.getErrors().size());
    assertTrue(r2.getErrors().get(0).contains("missing '='"));
  }

  // 字符串保护测试
  @Test
  public void testStringProtection() {
    String sql = "SET key = 'semi\\;colon';\n"
                 + "SELECT '--fake_comment', \"/*fake*/\";";

    SettingParser.ParseResult result = new SettingParser().extractSetStatements(sql);

    assertEquals("'semi;colon'", result.getSettings().get("key"));
    assertEquals("\nSELECT '--fake_comment', \"/*fake*/\";", result.getRemainingQuery());
  }

  @Test
  public void testSetLabel() {
    String sql = "SET odps.namespace.schema=true;\n"
                 + "                    SET LABEL 1 TO TABLE default.wrk_gh_events(`repo_id`, `repo_name`, `org_id`, `org_login`);";

    SettingParser.ParseResult result = new SettingParser().extractSetStatements(sql);
    System.out.println(result.getSettings());
    System.out.println(result.getErrors());
    System.out.println(result.getRemainingQuery());
  }

  // Test static parse method
  @Test
  public void testStaticParseMethod() {
    String sql = "SET key1=value1; SELECT 1;";
    SettingParser.ParseResult result = SettingParser.parse(sql);

    assertEquals(1, result.getSettings().size());
    assertEquals("value1", result.getSettings().get("key1"));
    assertEquals(" SELECT 1;", result.getRemainingQuery());
    assertTrue(result.getErrors().isEmpty());
  }

  // Test empty key error
  @Test
  public void testEmptyKeyError() {
    String sql = "SET =value;";
    SettingParser.ParseResult result = new SettingParser().extractSetStatements(sql);

    assertEquals(0, result.getSettings().size());
    assertEquals(1, result.getErrors().size());
    assertTrue(result.getErrors().get(0).contains("empty key"));
  }

  // Test multiple whitespace around equals
  @Test
  public void testMultipleWhitespaceAroundEquals() {
    String sql = "SET key1  =  value1; SET key2\t=\tvalue2;";
    SettingParser.ParseResult result = new SettingParser().extractSetStatements(sql);

    assertEquals(2, result.getSettings().size());
    assertEquals("value1", result.getSettings().get("key1"));
    assertEquals("value2", result.getSettings().get("key2"));
    assertTrue(result.getErrors().isEmpty());
  }

  // Test no settings in query
  @Test
  public void testNoSettingsInQuery() {
    String sql = "SELECT * FROM table;";
    SettingParser.ParseResult result = new SettingParser().extractSetStatements(sql);

    assertEquals(0, result.getSettings().size());
    assertEquals("SELECT * FROM table;", result.getRemainingQuery());
    assertTrue(result.getErrors().isEmpty());
  }

  // Test only settings in query
  @Test
  public void testOnlySettingsInQuery() {
    String sql = "SET key1=value1; SET key2=value2;";
    SettingParser.ParseResult result = new SettingParser().extractSetStatements(sql);

    assertEquals(2, result.getSettings().size());
    assertEquals("value1", result.getSettings().get("key1"));
    assertEquals("value2", result.getSettings().get("key2"));
    assertNull(result.getRemainingQuery()); // or check for empty string
  }

  // Test settings followed by whitespace only
  @Test
  public void testSettingsFollowedByWhitespace() {
    String sql = "SET key1=value1; SET key2=value2;   \n  \t  ";
    SettingParser.ParseResult result = new SettingParser().extractSetStatements(sql);

    assertEquals(2, result.getSettings().size());
    assertEquals("value1", result.getSettings().get("key1"));
    assertEquals("value2", result.getSettings().get("key2"));
  }

  // Test semicolon escaping in value
  @Test
  public void testSemicolonEscapingInValue() {
    String sql = "SET key1=val\\;ue1; SELECT 1;";
    SettingParser.ParseResult result = new SettingParser().extractSetStatements(sql);

    assertEquals(1, result.getSettings().size());
    assertEquals("val;ue1", result.getSettings().get("key1"));
    assertEquals(" SELECT 1;", result.getRemainingQuery());
  }

  // Test empty value
  @Test
  public void testEmptyValue() {
    String sql = "SET key1=;";
    SettingParser.ParseResult result = new SettingParser().extractSetStatements(sql);

    assertEquals(1, result.getSettings().size());
    assertEquals("", result.getSettings().get("key1"));
    assertTrue(result.getErrors().isEmpty());
  }

  // Test key with special characters
  @Test
  public void testKeyWithSpecialCharacters() {
    String sql = "SET key.with.dots=value1; SET key-with-dashes=value2;";
    SettingParser.ParseResult result = new SettingParser().extractSetStatements(sql);

    assertEquals(2, result.getSettings().size());
    assertEquals("value1", result.getSettings().get("key.with.dots"));
    assertEquals("value2", result.getSettings().get("key-with-dashes"));
  }

  // Test value with quotes
  @Test
  public void testValueWithQuotes() {
    String sql = "SET key1='quoted value'; SET key2=\"double quoted value\";";
    SettingParser.ParseResult result = new SettingParser().extractSetStatements(sql);

    assertEquals(2, result.getSettings().size());
    assertEquals("'quoted value'", result.getSettings().get("key1"));
    assertEquals("\"double quoted value\"", result.getSettings().get("key2"));
  }

  // Test complex SQL with multiple SET statements
  @Test
  public void testComplexSqlWithMultipleSetStatements() {
    String sql = "-- This is a comment\n"
                 + "SET project=odps_demo;  /* Another comment */\n"
                 + "SET table=users;\n"
                 + "/* Multi-line\n"
                 + "   comment */\n"
                 + "SET debug=true;\n"
                 + "SELECT * FROM ${project}.${table} WHERE debug = ${debug};";

    SettingParser.ParseResult result = new SettingParser().extractSetStatements(sql);

    assertEquals(3, result.getSettings().size());
    assertEquals("odps_demo", result.getSettings().get("project"));
    assertEquals("users", result.getSettings().get("table"));
    assertEquals("true", result.getSettings().get("debug"));
    assertTrue(result.getErrors().isEmpty());
  }

  // Test that 'set' inside a string is not parsed as a SET statement
  @Test
  public void testSetKeywordInString() {
    String sql = "SELECT 'this is a set of data' AS description; SET key=value;";
    SettingParser.ParseResult result = new SettingParser().extractSetStatements(sql);

    assertEquals(1, result.getSettings().size());
    assertEquals("value", result.getSettings().get("key"));
    assertEquals("SELECT 'this is a set of data' AS description; ", result.getRemainingQuery());
  }

  // Test ParseResult getters
  @Test
  public void testParseResultGetters() {
    Map<String, String> settings = new java.util.HashMap<>();
    settings.put("key1", "value1");
    String remainingQuery = "SELECT 1;";
    List<String> errors = new java.util.ArrayList<>();
    errors.add("test error");

    SettingParser.ParseResult result = new SettingParser.ParseResult(settings, remainingQuery, errors);

    assertEquals(settings, result.getSettings());
    assertEquals(remainingQuery, result.getRemainingQuery());
    assertEquals(errors, result.getErrors());
  }

  // Test query without trailing semicolon gets one added
  @Test
  public void testQueryWithoutTrailingSemicolon() {
    String sql = "SET key1=value1";
    SettingParser.ParseResult result = SettingParser.parse(sql);

    // Should have error about missing semicolon
    assertEquals(1, result.getErrors().size());
    assertTrue(result.getErrors().get(0).contains("missing semicolon"));
  }
}
