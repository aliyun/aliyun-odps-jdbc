package com.aliyun.odps.main;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.aliyun.odps.commons.util.IOUtils;
import com.aliyun.odps.ext.StatementWrapper;
import com.aliyun.odps.jdbc.OdpsAsyncStatement;
import com.aliyun.odps.jdbc.OdpsConnection;
import com.aliyun.odps.jdbc.OdpsDriver;
import com.aliyun.odps.util.SqlRunUtils;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.output.NullPrintStream;
import org.apache.commons.lang.StringUtils;

/**
 * benchmark client entry
 *
 * @author biliang.wbl
 * @date 2025/07/17
 */
public class Main {
    private static final PrintStream NULL = NullPrintStream.NULL_PRINT_STREAM;

    public static void main(String[] args) throws Exception {

        Args arg = new Args();
        try {
            arg.parse(args);
        } catch (ParseException e) {
            e.printStackTrace();
            System.err.println(
                "Usage : java -jar benchmark-client-1.0.jar -c [config_file] -s [global_setting_file] -i [index_file]");
            System.exit(1);
        }
        System.err.println(arg);

        Config config = Config.getInstance();
        config.load(arg.getConfigFilePath());
        //System.out.println(config);

        // create jdbc connection with global setting
        Properties globalSetting = null;
        if (!arg.getGlobalSettingFile().isEmpty()) {
            globalSetting = loadGlobalSetting(arg.getGlobalSettingFile());
        }

        List<QueryFileObject> fileList = buildFileList(arg.getIndexFilePath());
        System.err.println("FileNameList:");
        for (QueryFileObject file : fileList) {
            System.err.println(file.fileName);
        }

        try (OdpsConnection conn = createConn(config, globalSetting)) {
            for (QueryFileObject file : fileList) {
                executePerFile(conn, file);
            }
        }
    }

    private static void executePerFile(OdpsConnection conn, QueryFileObject file) throws SQLException {
        System.out.println("fileName: " + file.fileName);
        System.out.println("SQL:\n" + file.originQueryText);

        System.err.println("fileName: " + file.fileName);
        System.err.println("SQL:\n" + file.originQueryText);

        // execute each sql file with one OdpsStatement
        // Ensure that the flag setting in each SQL file affects only the current file.
        try (OdpsAsyncStatement statement = (OdpsAsyncStatement)conn.createStatement()) {
            StatementWrapper statementWrapper = new StatementWrapper(statement);
            int sqlNo = 1;
            for (String sql : file.sqlList) {
                boolean isNormalQuery = isNormalQuery(sql);
                PrintStream stdout = isNormalQuery ? System.out : NULL;
                PrintStream stderr = isNormalQuery ? System.err : NULL;

                stdout.println("Index :" + sqlNo);
                stderr.println("Index :" + sqlNo);

                if (isNormalQuery) {
                    sqlNo++;
                }
                try {
                    statementWrapper.executeForTest(sql, stdout, stderr);
                } catch (Exception e) {
                    //System.err.println("run query error for SQL: " + sql);
                    e.printStackTrace();
                }
            }
        }
    }

    private static boolean isNormalQuery(String sql) {
        String lcSql = sql.trim().toLowerCase();
        return !(lcSql.startsWith("set") || lcSql.startsWith("unset"));
    }

    private static OdpsConnection createConn(Config config, Properties globalSetting) throws SQLException {
        String jdbcUrl = config.buildJdbcUrl();
        System.err.println("JdbcUrl: " + jdbcUrl);

        OdpsDriver driver = null;// load class
        OdpsConnection conn = (OdpsConnection)DriverManager.getConnection(jdbcUrl, config.getConfigProps());

        if (globalSetting != null) {
            conn.getSqlTaskProperties().putAll(globalSetting);
        }
        return conn;
    }

    private static Properties loadGlobalSetting(String globalSettingFile) throws IOException {
        Properties props = new Properties();
        try (FileReader reader = new FileReader(globalSettingFile)) {
            props.load(reader);
        }
        return props;
    }

    private static List<QueryFileObject> buildFileList(String indexFilePath) {
        List<QueryFileObject> fileList = new ArrayList<>();
        try (FileReader r = new FileReader(indexFilePath); BufferedReader reader = new BufferedReader(r)) {
            String fileName = null;
            while ((fileName = reader.readLine()) != null) {
                if (StringUtils.isNotEmpty(fileName)) {
                    fileList.add(new QueryFileObject(fileName, readContent(fileName.trim())));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return fileList;
    }

    private static String readContent(String fileName) throws IOException {
        try (FileInputStream fis = new FileInputStream(fileName)) {
            return IOUtils.readStreamAsString(fis);
        }
    }

    private static class QueryFileObject {
        private String fileName;
        private String originQueryText;
        private List<String> sqlList;

        public QueryFileObject(String fileName, String originQueryText) {
            this.fileName = fileName;
            this.originQueryText = originQueryText;
            this.sqlList = SqlRunUtils.parseCommand(originQueryText);
        }
    }
}
