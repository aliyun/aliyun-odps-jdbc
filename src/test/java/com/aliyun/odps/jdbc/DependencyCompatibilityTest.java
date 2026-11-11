package com.aliyun.odps.jdbc;

import static org.junit.jupiter.api.Assertions.*;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.commons.proto.ProtobufRecordStreamReader;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.fasterxml.jackson.core.ErrorReportConfiguration;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.luben.zstd.Zstd;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.github.luben.zstd.ZstdDictCompress;
import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import net.jpountz.lz4.LZ4FrameInputStream;
import net.jpountz.lz4.LZ4FrameOutputStream;
import org.junit.jupiter.api.Test;
import org.xerial.snappy.Snappy;

/** Offline compatibility checks for the libraries embedded by the JDBC shade build. */
public class DependencyCompatibilityTest {
  @Test
  void jacksonDataInputLimitsInvalidTokenInErrorMessage() throws IOException {
    // CVE-2026-89425: the DataInput parser must honor the same error-token
    // limit as the other parsers rather than retaining the entire bad token.
    byte[] input = new byte[100000];
    java.util.Arrays.fill(input, (byte) 'x');
    input[input.length - 1] = ' ';
    JsonFactory factory = JsonFactory.builder().errorReportConfiguration(
        ErrorReportConfiguration.builder().maxErrorTokenLength(32).build()).build();
    try (JsonParser parser = factory.createParser(
        (DataInput) new DataInputStream(new ByteArrayInputStream(input)))) {
      JsonParseException failure = assertThrows(JsonParseException.class, parser::nextToken);
      assertTrue(failure.getMessage().length() < 1024,
          "Malformed input must not produce an unbounded error message");
    }
  }

  @Test
  void jacksonRoundTripsSdkStyleJson() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode input = mapper.readTree("{\"Name\":\"jdbc-查询\",\"Count\":42,\"Items\":[true,null]}");
    assertEquals(input, mapper.readTree(mapper.writeValueAsBytes(input)));
  }

  @Test
  void zstdStreamRoundTrip() throws IOException {
    byte[] input = "MaxCompute JDBC Zstandard regression 查询".getBytes(StandardCharsets.UTF_8);
    ByteArrayOutputStream encoded = new ByteArrayOutputStream();
    try (ZstdOutputStream out = new ZstdOutputStream(encoded)) {
      out.write(input);
    }
    ByteArrayOutputStream decoded = new ByteArrayOutputStream();
    try (ZstdInputStream in = new ZstdInputStream(new ByteArrayInputStream(encoded.toByteArray()))) {
      byte[] buffer = new byte[7];
      int size;
      while ((size = in.read(buffer)) != -1) {
        decoded.write(buffer, 0, size);
      }
    }
    assertArrayEquals(input, decoded.toByteArray());
  }

  @Test
  @SuppressWarnings("deprecation")
  void zstdRejectsInvalidOffsetsBeforeCallingNativeCode() {
    byte[] input = new byte[16];
    assertThrows(IndexOutOfBoundsException.class, () -> Zstd.getFrameContentSize(input, -1, 4));
    assertThrows(IndexOutOfBoundsException.class, () -> Zstd.decompressedSize(input, -1, 4));
    assertThrows(IllegalArgumentException.class, () -> new ZstdDictCompress(input, -1, 4, 3));
    assertThrows(IllegalArgumentException.class, () -> new ZstdDictCompress(input, 0, 17, 3));
  }

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
  void snappyTypedArrayRoundTripsMatchTunnelCodecUsage() throws IOException {
    // The Tunnel codecs call the typed-array helpers; keep them working on the bundled copy.
    char[] chars = "MaxCompute JDBC snappy regression 查询".toCharArray();
    assertArrayEquals(chars, Snappy.uncompressCharArray(Snappy.compress(chars)));
    int[] ints = {0, -1, 42, Integer.MAX_VALUE, Integer.MIN_VALUE};
    assertArrayEquals(ints, Snappy.uncompressIntArray(Snappy.compress(ints)));
    long[] longs = {0L, -1L, Long.MAX_VALUE, Long.MIN_VALUE};
    assertArrayEquals(longs, Snappy.uncompressLongArray(Snappy.compress(longs)));
    byte[] payload = Snappy.compress("tunnel-frame".getBytes(StandardCharsets.UTF_8));
    assertArrayEquals("tunnel-frame".getBytes(StandardCharsets.UTF_8), Snappy.uncompress(payload));
  }

  @Test
  void snappyRejectsTruncatedFramesBeforeNativeCopy() throws IOException {
    // CVE-2026-108106: a declared uncompressed length must not drive an unbounded allocation.
    byte[] payload = Snappy.compress("tunnel-frame".getBytes(StandardCharsets.UTF_8));
    assertFalse(Snappy.isValidCompressedBuffer(new byte[] {1, 2, 3, 4, 5, 6, 7, 8}));
    assertThrows(IOException.class,
        () -> Snappy.uncompressedLength(new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff}));
    assertThrows(IOException.class, () -> Snappy.uncompress(new byte[] {payload[0]}));
  }

  @Test
  void snappyArrayCopyRejectsOutOfRangeArguments() throws IOException {
    // CVE-2026-116590: snappy-java before 1.1.10.9 copied without validating the destination,
    // so these calls wrote past the target array instead of failing.
    byte[] payload = Snappy.compress("tunnel-frame".getBytes(StandardCharsets.UTF_8));
    assertThrows(IllegalArgumentException.class,
        () -> Snappy.arrayCopy(payload, 0, payload.length, new byte[1], 0));
    assertThrows(IllegalArgumentException.class,
        () -> Snappy.arrayCopy(payload, -1, payload.length, new byte[64], 0));
  }

  @Test
  void guavaCacheAndImmutableCollectionsUsedBySdkResolve() {
    // Core uses CacheBuilder for the Tunnel endpoint cache and ImmutableList/Map/Lists in
    // metadata paths; pin the surface the bundled guava copy has to keep providing.
    Cache<String, String> cache = CacheBuilder.newBuilder().maximumSize(8).build();
    cache.put("tunnel-endpoint", "http://dt.example.invalid");
    assertEquals("http://dt.example.invalid", cache.getIfPresent("tunnel-endpoint"));
    assertEquals(ImmutableMap.of("name", "value"),
        ImmutableMap.copyOf(java.util.Collections.singletonMap("name", "value")));
    assertEquals(2, Lists.asList("a", new String[] {"b"}).size());
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
