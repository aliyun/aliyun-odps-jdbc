package com.aliyun.odps.jdbc.utils;


import org.junit.Test;
import static org.junit.Assert.*;
import java.util.Map;

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
}
