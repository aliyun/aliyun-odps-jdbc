package com.aliyun.odps.util;

import java.util.List;

import com.google.common.collect.Lists;

public class SqlRunUtils {
    public static List<String> parseCommand(String commandLines) {
        commandLines = commandLines.trim();
        // 命令行，可支持多个命令
        List<String> commandLinelist = new AntlrObject(commandLines).splitCommands();
        // 如果SqlLinesParser解析出来为空，且与“--”开始，是引号不匹配,这种场景,把query发给相应的command自己处理
        if (commandLinelist.isEmpty() && !commandLines.startsWith("--")) {
            int index = commandLines.lastIndexOf(";");
            if (index != -1) {
                return Lists.newArrayList(commandLines.substring(0, index));
            } else {
                return Lists.newArrayList(commandLines);
            }
        }
        return commandLinelist;
    }
}
