package com.aliyun.odps.jdbc.types;

import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aliyun.credentials.api.ICredentials;
import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.credentials.StaticCredentialProvider;
import com.aliyun.odps.data.Blob;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.jdbc.OdpsResultSetMetaData;
import com.aliyun.odps.storage.MaxStorageClient;
import com.aliyun.odps.storage.write.TableWriteSession;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.arrow.ArrowWriter;
import com.aliyun.odps.type.TypeInfo;

import org.junit.jupiter.api.Disabled;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 * cannot run yet
 */
@Disabled
public class BlobTest {

  private static Odps odps;

  private static Connection connection;

  @BeforeAll
  public static void initClient() throws Exception {

    Account account =
      new AliyunAccount("", "");
    odps = new Odps(account);
    odps.setDefaultProject("avro_test");
    odps.setEndpoint("http://100.69.248.78:8002/odps_dailyrunnew");

    ICredentials credentials = account.getCredentials();

    String tunnelEndpoint = "http://11.163.118.159:18091";

    String url = String.format("jdbc:odps:%s?project=%s&accessId=%s&accessKey=%s&tunnelEndpoint=%s&interactive_mode=true",
                               odps.getEndpoint(), odps.getDefaultProject(), credentials.getAccessKeyId(), credentials.getAccessKeySecret(), tunnelEndpoint);
    Class.forName("com.aliyun.odps.jdbc.OdpsDriver");

    connection = DriverManager.getConnection(url);

    if (!odps.tables().exists("jdbc_blob_test")) {

      String str = "hello world! this is a blob!";

      Map<String, String> hints = new HashMap<>();
      hints.put("odps.table.append2.enable", "true");
      hints.put("odps.sql.type.system.odps2", "true");

      Statement statement = connection.createStatement();

      statement.execute("set odps.table.append2.enable = true;"
                        + "set odps.sql.type.system.odps2 = true;" +
                        "CREATE TABLE IF NOT EXISTS jdbc_blob_test(c1 BIGINT, c2 BLOB) TBLPROPERTIES ('table.format.version'='2','transactional'='true');");

      MaxStorageClient client = MaxStorageClient.builder()
        .endpoint(odps.getEndpoint())
        .tunnelEndpoint(odps.getTunnelEndpoint()) // Use the tunnel endpoint if available
        .credentialsProvider(new StaticCredentialProvider(odps.getAccount().getCredentials()))
        .project(odps.getDefaultProject())
        .build();

      TableWriteSession writeSession =
        client.createTableWriteSessionBuilder(
            TableIdentifier.of(odps.getDefaultProject(), "jdbc_blob_test"))
          .withOverwrite(true)
          .build();

      ArrowWriter arrowWriter = writeSession.createWriterBuilder("test", 1)
        .withBufferSize(4 * 1024 * 1024)
        .build();

      RecordWriter writer = arrowWriter.getAsRecordWriter(1000);

      Blob blob = Blob.fromInputStream(new ByteArrayInputStream(str.getBytes()));

      Record record = writer.newRecord(false);
      record.set(0, 123456L);
      record.set(1, blob);
      writer.write(record);
      writer.close();

      writeSession.commit();
    }
  }

  @Test
  public void testSelectBlobs() throws Exception {

    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("select * from jdbc_blob_test;");

    ResultSetMetaData metaData = resultSet.getMetaData();
    int columnType = metaData.getColumnType(2);
    System.out.println(columnType);

    String columnTypeName = metaData.getColumnTypeName(2);
    System.out.println(columnTypeName);

    TypeInfo columnOdpsType = ((OdpsResultSetMetaData) metaData).getColumnOdpsType(2);
    System.out.println(columnOdpsType.getTypeName());

    while (resultSet.next()) {
      System.out.println(resultSet.getString(2));
    }
  }
}
