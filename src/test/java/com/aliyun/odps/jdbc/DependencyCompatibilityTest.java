package com.aliyun.odps.jdbc;

import static org.junit.jupiter.api.Assertions.*;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.commons.proto.ProtobufRecordStreamReader;
import com.aliyun.odps.tunnel.io.CompressOption;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import net.jpountz.lz4.LZ4FrameInputStream;
import net.jpountz.lz4.LZ4FrameOutputStream;
import org.junit.jupiter.api.Test;

/** Offline compatibility checks for the libraries embedded by the JDBC shade build. */
public class DependencyCompatibilityTest {
  @Test
  void nettyClientDecodesFragmentedResponse() {
    EmbeddedChannel channel = new EmbeddedChannel(new HttpClientCodec(), new HttpObjectAggregator(65536));
    try {
      assertFalse(channel.writeInbound(Unpooled.copiedBuffer("HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhe", StandardCharsets.US_ASCII)));
      assertTrue(channel.writeInbound(Unpooled.copiedBuffer("llo", StandardCharsets.US_ASCII)));
      FullHttpResponse response = channel.readInbound();
      try {
        assertEquals(200, response.status().code());
        assertEquals("hello", response.content().toString(StandardCharsets.UTF_8));
      } finally {
        response.release();
      }
    } finally {
      channel.finishAndReleaseAll();
    }
  }

  @Test
  void lz4FrameRoundTrip() throws IOException {
    byte[] input = "MaxCompute JDBC dependency regression".getBytes(StandardCharsets.UTF_8);
    ByteArrayOutputStream encoded = new ByteArrayOutputStream();
    try (LZ4FrameOutputStream out = new LZ4FrameOutputStream(encoded)) {
      out.write(input);
    }
    ByteArrayOutputStream decoded = new ByteArrayOutputStream();
    try (LZ4FrameInputStream in = new LZ4FrameInputStream(new ByteArrayInputStream(encoded.toByteArray()))) {
      byte[] buffer = new byte[7];
      int size;
      while ((size = in.read(buffer)) != -1) {
        decoded.write(buffer, 0, size);
      }
    }
    assertArrayEquals(input, decoded.toByteArray());
  }

  @Test
  void tunnelRejectsUnknownGroupBeforeGenericProtobufParsing() throws IOException {
    // field 15, wire type START_GROUP; repeated groups must not reach UnknownFieldSet.
    byte[] input = new byte[8192];
    java.util.Arrays.fill(input, (byte) 0x7b);
    com.aliyun.odps.TableSchema schema = new com.aliyun.odps.TableSchema();
    schema.addColumn(new Column("value", OdpsType.STRING));
    try (ProtobufRecordStreamReader reader = new ProtobufRecordStreamReader(
        schema, new ByteArrayInputStream(input),
        new CompressOption(CompressOption.CompressAlgorithm.ODPS_RAW, -1, 0))) {
      IOException failure = assertThrows(IOException.class, reader::read);
      assertTrue(failure.getMessage().contains("Invalid protobuf tag"));
    }
  }
}
