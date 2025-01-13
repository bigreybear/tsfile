package optimize.util;

import java.util.Arrays;
import java.util.Random;
import java.util.stream.IntStream;

public class ArrayHelper {

  public static byte[] removeTrailingZeros(byte[] src) {
    int i = 1;
    while (i < src.length && src[i] != 0) i++;
    return Arrays.copyOfRange(src, 0, i);
  }

  public static byte[] removeTrailing(byte[] src, byte b) {
    int i = 0;
    while (i < src.length && src[i] != b) i++;
    return Arrays.copyOfRange(src, 0, i);
  }

  // find all positions from start(inclusive) to end(inclusive) except those in exp which is ordered
  public static int[] findComplementary(int start, int end, int[] exp) {
    if (start > end) throw new RuntimeException();
    if (start == end) return new int[0];

    // pre-allocate a larger result, preferring time other than space
    int[] r = new int[end - start + 1];
    if (exp == null || exp.length == 0) {
      for (int i = start; i <= end; i++) {
        r[i - start] = i;
      }
      return r;
    }

    int ri = 0, bl = exp.length;
    for (int i = start, bi = 0; i <= end; i++) {
      while (bi < bl && exp[bi] < i) bi++; // find the exp inside the interested scope
      if (bi < bl && i == exp[bi]) continue; // continue if the current i is excepted
      r[ri++] = i;
    }
    return ri == 0 ? new int[0] : Arrays.copyOfRange(r, 0, ri);
  }

  // only for test
  private static int[] generateSortedIntArray(int size, int min, int max) {
    Random rand = new Random();
    return IntStream.generate(() -> rand.nextInt(max - min + 1) + min)
        .distinct() // 如果需要唯一元素
        .limit(size)
        .sorted()
        .toArray();
  }

  private static int[] generateFullArray(int first, int last) {
    int[] r = new int[last - first + 1];
    for (int i = 0; i < r.length; i++) {
      r[i] = i + first;
    }
    return r;
  }

  public static void main(String[] args) {
    System.out.println(Arrays.toString(findComplementary(3, 10, new int[] {1, 2, 5, 6, 7, 12})));

    int[] arr = generateSortedIntArray(50, 1, 125);
    int[] itv, comp = generateFullArray(arr[0], arr[arr.length - 1]);

    System.out.println(
        Arrays.compare(
            // semantically equivalent
            findComplementary(arr[0], arr[arr.length - 1], arr), itv = findIntervals(arr)));
    System.out.println(Arrays.compare(mergeIntArr(itv, arr), comp));
  }

  // a wrapper
  public static int[] findIntervals(byte[] pos) {
    int[] res = new int[pos.length];
    for (int i = 0; i < pos.length; i++) {
      res[i] = 0xff & pos[i];
    }
    return findIntervals(res);
  }

  // shall not be used in search for perf. issue
  public static int[] findIntervals(int[] pos) {
    if (pos == null || pos.length <= 1) return new int[0];

    checkIntArrSorted(pos);
    // must be ordered and deduplicated
    int[] itvPos = new int[pos[pos.length - 1] - (pos[0] - 1) - pos.length];
    for (int idx = 1, k = 0; idx < pos.length; ) {
      // k records number in itvPos
      if (pos[idx - 1] + 1 != pos[idx]) {
        for (int pi = pos[idx - 1] + 1; pi < pos[idx]; pi++) itvPos[k++] = pi;
      }
      idx++;
    }
    return itvPos;
  }

  private static void checkIntArrSorted(final int[] a) {
    for (int i = 1; i < a.length; i++)
      if (a[i - 1] >= a[i]) throw new RuntimeException("Array not sorted.");
  }

  public static int[] mergeIntArr(int[] a, int[] b) {
    if (a.length == 0) return b;
    if (b.length == 0) return a;
    checkIntArrSorted(a);
    checkIntArrSorted(b);
    int[] r = new int[a.length + b.length];
    int ai = 0, bi = 0;
    while (ai < a.length && bi < b.length) {
      if (a[ai] == b[bi]) throw new RuntimeException("Merging arrays overlapped.");
      r[ai + bi] = a[ai] < b[bi] ? a[ai++] : b[bi++];
    }
    if (ai < a.length) {
      System.arraycopy(a, ai, r, ai + bi, a.length - ai);
    } else if (bi < b.length) {
      System.arraycopy(b, bi, r, ai + bi, b.length - bi);
    }
    return r;
  }
}
