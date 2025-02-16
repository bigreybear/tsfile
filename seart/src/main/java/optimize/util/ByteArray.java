package optimize.util;

import java.util.Arrays;
import java.util.List;

// A hashable byte array, can be key in hash map
public class ByteArray implements Comparable<ByteArray> {
  final byte[] val;

  public ByteArray() {
    val = null;
  }

  public ByteArray(ByteArray ori, byte add) {
    val = new byte[ori.val == null ? 1 : (ori.val.length + 1)];
    if (ori.val != null) System.arraycopy(ori.val, 0, val, 0, ori.val.length);
    val[val.length - 1] = add;
  }

  public ByteArray(byte[] ba) {
    val = ba;
  }

  public ByteArray(byte a) {val = new byte[] {a};}

  public byte[] getVal() {
    return val;
  }

  public ByteArray getSlice(int s, int e) {
    return new ByteArray(Arrays.copyOfRange(val, s, e));
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof ByteArray && Arrays.equals(val, ((ByteArray) o).val);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(val);
  }

  @Override
  public String toString() {
    return val == null ? "" : Arrays.toString(val);
  }

  public static byte[] concatenate(byte[] pre, byte[] suc) {
    if (pre == null || suc == null) throw new RuntimeException();

    byte[] tar = new byte[pre.length + suc.length];
    System.arraycopy(pre, 0, tar, 0, pre.length);
    System.arraycopy(suc, 0, tar, pre.length, suc.length);
    return tar;
  }

  public static byte[] concatenate(byte pre, byte[] suc) {
    byte[] p = new byte[1];
    p[0] = pre;
    return concatenate(p, suc);
  }

  public static byte[] concatenate(byte[] pre, byte suc) {
    byte[] s = new byte[1];
    s[0] = suc;
    return concatenate(pre, s);
  }

  public static ByteArray join(byte[] ks, byte sep) {
    int tarLen = 2 * ks.length - 1;
    byte[] res = new byte[tarLen];
    Arrays.sort(res);
    for (int i = 0, curLen = 0; i < res.length; i++) {
      res[curLen++] = ks[i];
      if (curLen == tarLen) break;
      res[curLen++] = sep;
    }
    return new ByteArray(res);
  }

  public static ByteArray join(byte[][] b, byte sep) {
    return join(Arrays.asList(b), sep);
  }

  public static ByteArray join(List<byte[]> src, byte sep) {
    src.sort(Arrays::compare);
    byte[] res = new byte[src.stream().mapToInt(i1 -> i1.length).sum() + src.size() - 1];
    for (int i = 0, curLen = 0; i < src.size(); i++) {
      System.arraycopy(src.get(i), 0, res, curLen, src.get(i).length);
      curLen += src.get(i).length;
      if (curLen == res.length) return new ByteArray(res);
      res[curLen++] = sep;
    }
    throw new RuntimeException();
  }

  @Override
  public int compareTo(ByteArray other) {
    int minLen = Math.min(val.length, other.val.length);
    for (int i = 0; i < minLen; i++) {
      int diff = Byte.compare(val[i], other.val[i]);
      if (diff != 0) {
        return diff;
      }
    }
    return Integer.compare(val.length, other.val.length);
  }
}
