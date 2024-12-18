package optimize.util;

import java.util.Arrays;

public class ArrayHelper {

  public static boolean identical(byte[] a, byte[] b) {
    if (a == null && b == null) return true;
    if (a == null ^ b == null) return false;
    if (a.length != b.length) return false;

    for (int i = 0; i < a.length; i++) {
      if (a[i] != b[i]) return false;
    }
    return true;
  }

  public static byte[] removeTrailingZeros(byte[] src) {
    int i = 0;
    while (i < src.length && src[i] != 0) i++;
    return Arrays.copyOfRange(src, 0, i);
  }

  public static byte[] removeTrailing(byte[] src, byte b) {
    int i = 0;
    while (i < src.length && src[i] != b) i++;
    return Arrays.copyOfRange(src, 0, i);
  }
}
