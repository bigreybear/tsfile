package optimize.nodes.cdm;

import static optimize.util.ArrayHelper.removeTrailingZeros;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

// all numerical encoded as BIG_ENDIAN
public class ByteEncode {
  public static Charset coding = StandardCharsets.UTF_8;

  // region String Utils
  public static byte[][] strings2ByteArrays(List<String> s) {
    byte[][] res = new byte[s.size()][];
    Arrays.parallelSetAll(res, i -> s.get(i).getBytes(coding));
    return res;
  }

  public static byte[][] strings2ByteArrays(String[] s) {
    byte[][] res = new byte[s.length][];
    Arrays.parallelSetAll(res, i -> s[i].getBytes(coding));
    return res;
  }

  static String[] bytes2Strings(byte[][] b) {
    String[] r = new String[b.length];
    Arrays.parallelSetAll(r, i -> new String(b[i], coding));
    return r;
  }

  // endregion

  public static void main(String[] args) {
    Random random = new Random();
    int loop = 0;
    while (loop++ < 10000) {
      int i1 = random.nextInt(), i2 = random.nextInt();
      long l = (long) i1 << 32 | i2;
      short s = (short) (i1 & 0xffff);

      if (i1 != bytes2Int(int2Bytes(i1)) || i2 != bytes2Int(int2Bytes(i2)))
        System.out.println("WRONG");

      if (s != bytes2Short(short2Bytes(s))) System.out.println("WRONG");

      if (l != bytes2Long(long2Bytes(l))) System.out.println("WRONG");
    }
    System.out.println("HELLO");
  }

  public static int validLength(final long v, final int bitWid) {
    if (bitWid != 2 && bitWid != 4 && bitWid != 8) throw new UnsupportedOperationException();
    for (int i = 2; i <= bitWid; i++) {
      if ((v >> ((bitWid - i) << 3) & 0xff) == 0) return i - 1;
    }
    return bitWid;
  }

  public static long bytes2Long(byte[] b) {
    int len = b.length;
    if (len > 8)
      throw new UnsupportedOperationException("5 or more bytes cannot be encoded to an int.");
    long result = 0;
    switch (len) {
      case 8:
        result |= (b[7] & 0xff);
      case 7:
        result |= (b[6] & 0xff) << 8;
      case 6:
        result |= (b[5] & 0xff) << 16;
      case 5:
        result |= (long) (b[4] & 0xff) << 24;
      case 4:
        result |= (long) (b[3] & 0xff) << 32;
      case 3:
        result |= (long) (b[2] & 0xff) << 40;
      case 2:
        result |= (long) (b[1] & 0xff) << 48;
      case 1:
        result |= (long) (b[0] & 0xff) << 56;
        break;
      case 0:
        break;
    }
    return result;
  }

  public static int bytes2Int(byte[] b) {
    int len = b.length;
    if (len > 4)
      throw new UnsupportedOperationException("5 or more bytes cannot be encoded to an int.");
    int result = 0;
    switch (len) {
      case 4:
        result |= (b[3] & 0xFF);
      case 3:
        result |= (b[2] & 0xFF) << 8;
      case 2:
        result |= (b[1] & 0xFF) << 16;
      case 1:
        result |= (b[0] & 0xFF) << 24;
        break;
      case 0:
        break;
    }
    return result;
  }

  public static short bytes2Short(byte[] b) {
    int len = b.length;
    if (len > 2)
      throw new UnsupportedOperationException("5 or more bytes cannot be encoded to an int.");
    short result = 0;
    switch (len) {
      case 2:
        result |= (short) (b[1] & 0xFF);
      case 1:
        result |= (short) ((b[0] & 0xFF) << 8);
        break;
      case 0:
        break;
    }
    return result;
  }

  public static byte[] long2Bytes(final long i) {
    byte[] b = new byte[8];
    b[7] = (byte) (i & 0xff);
    b[6] = (byte) (i >> 8 & 0xff);
    b[5] = (byte) (i >> 16 & 0xff);
    b[4] = (byte) (i >> 24 & 0xff);
    b[3] = (byte) (i >> 32 & 0xff);
    b[2] = (byte) (i >> 40 & 0xff);
    b[1] = (byte) (i >> 48 & 0xff);
    b[0] = (byte) (i >> 56 & 0xff);
    return b;
  }

  // all bytes are processed as is, for fixed length
  public static byte[] int2Bytes(final int i) {
    byte[] b = new byte[4];
    b[3] = (byte) (i & 0xff);
    b[2] = (byte) (i >> 8 & 0xff);
    b[1] = (byte) (i >> 16 & 0xff);
    b[0] = (byte) (i >> 24 & 0xff);
    return b;
  }

  public static byte[] short2Bytes(final short i) {
    byte[] b = new byte[2];
    b[1] = (byte) (i & 0xff);
    b[0] = (byte) (i >> 8 & 0xff);
    return b;
  }

  /** zero-byte is valid only when it is the first byte. */
  public static byte[] int2BytesNoTrailing(final int i) {
    return removeTrailingZeros(int2Bytes(i));
  }

  public static byte[] short2BytesNoTrailing(final short s) {
    return removeTrailingZeros(short2Bytes(s));
  }

  public static byte[] long2BytesNoTrailing(final long l) {
    return removeTrailingZeros(long2Bytes(l));
  }
}
