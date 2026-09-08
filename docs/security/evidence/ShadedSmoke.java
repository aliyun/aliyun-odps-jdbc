import com.aliyun.odps.jdbc.shaded.io.netty.buffer.ByteBuf;
import com.aliyun.odps.jdbc.shaded.io.netty.buffer.Unpooled;
import com.aliyun.odps.jdbc.shaded.com.google.protobuf.CodedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import net.jpountz.lz4.LZ4FrameInputStream;
import net.jpountz.lz4.LZ4FrameOutputStream;
public class ShadedSmoke {
  public static void main(String[] args) throws Exception {
    ByteBuf b = Unpooled.buffer(8);
    try { b.writeLong(123L); if (b.readLong() != 123L) throw new AssertionError(); }
    finally { b.release(); }
    if (CodedInputStream.newInstance(new byte[] {8, 1}).readTag() != 8) throw new AssertionError();
    byte[] source = "JDBC shaded runtime".getBytes("UTF-8");
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (LZ4FrameOutputStream out = new LZ4FrameOutputStream(bytes)) { out.write(source); }
    ByteArrayOutputStream result = new ByteArrayOutputStream();
    try (LZ4FrameInputStream in = new LZ4FrameInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      int c; while ((c = in.read()) != -1) result.write(c);
    }
    if (!Arrays.equals(source, result.toByteArray())) throw new AssertionError();
    System.out.println("PASS shaded Netty buffer, shaded protobuf scalar, LZ4 frame round-trip");
  }
}
