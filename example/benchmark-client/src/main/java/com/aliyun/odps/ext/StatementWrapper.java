package com.aliyun.odps.ext;

import java.io.PrintStream;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;

import com.aliyun.odps.Instance;
import com.aliyun.odps.jdbc.OdpsAsyncStatement;

public class StatementWrapper {
    private final OdpsAsyncStatement statement;
    private final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public StatementWrapper(OdpsAsyncStatement statement) {
        this.statement = statement;
    }

    public void executeForTest(String sql, PrintStream stdout, PrintStream errout)
        throws Exception {
        long start = System.currentTimeMillis();

        boolean hasResult = false;
        try {
            hasResult = statement.execute(sql);
        } catch (Throwable e) {
            errout.println(e.getMessage());
        }
        // print logview before finish
        printLogview(errout, statement);

        waitForFinish(errout, statement, start);

        printResultSet(stdout, hasResult, statement);
    }

    private void printLogview(PrintStream errout, OdpsAsyncStatement statement) {
        errout.println("Log view:\n" + statement.getLogViewUrl());
    }

    private void waitForFinish(PrintStream errout, OdpsAsyncStatement statement, long start) {
        Instance instance = statement.getExecuteInstance();
        if (instance != null) {
            // Check status every 50 milliseconds
            try {
                instance.waitForSuccess(50L);
            } catch (Throwable e) {
                errout.println(e.getMessage());
            }
        }
        long end = System.currentTimeMillis();

        errout.println("starttime:" + format.format(new Date(start)));
        errout.println("endtime:" + format.format(new Date(end)));

        errout.println("Cost time : " + (end - start) + " ms");
        errout.println();
    }

    private void printResultSet(PrintStream stdout, boolean hasResult, Statement statement) throws SQLException {
        stdout.println("Result:");
        if (!hasResult) {
            return;
        }
        ResultSet resultSet = statement.getResultSet();
        if (resultSet == null) {
            return;
        }

        ResultSetMetaData metadata = resultSet.getMetaData();
        int columnCount = metadata.getColumnCount();
        StringBuilder head = new StringBuilder();
        for (int index = 1; index <= columnCount; index++) {
            String columnName = metadata.getColumnName(index);
            head.append(columnName).append("\t");
        }
        stdout.println(head.toString());
        while (true) {
            boolean hasNext = false;
            try {
                hasNext = resultSet.next();
            } catch (Throwable e) {}

            if (!hasNext) {
                break;
            }

            for (int i = 0; i < columnCount; i++) {
                stdout.print(resultSet.getString(i + 1));
                stdout.print("\t");
            }
            stdout.println();
        }
    }
}
