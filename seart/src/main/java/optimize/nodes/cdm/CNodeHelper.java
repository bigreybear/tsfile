package optimize.nodes.cdm;

import static optimize.nodes.cdm.ByteEncode.bytes2Int;
import static optimize.nodes.cdm.ByteEncode.int2BytesNoTrailing;
import static optimize.util.ArrayHelper.findIntervals;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import loader.PathTxtLoader;
import optimize.nodes.cdm.hashed.HCNode2;
import optimize.nodes.cdm.hashed.HCNode4;
import optimize.nodes.cdm.hashed.HCNode8;
import optimize.nodes.cdm.one.CNode1F256;
import optimize.nodes.cdm.one.CNode1F48;
import optimize.nodes.cdm.one.CNode1FBS;
import optimize.nodes.cdm.sorted.SCNode2;
import optimize.nodes.cdm.sorted.SCNode4;
import optimize.nodes.cdm.sorted.SCNode8;

public class CNodeHelper {
  public static final int POS_SIZE = 4;

  // public static final byte[]

  // may extract 0 bytes
  public static byte[] extractBytes(byte[] arr, int[] pos) {
    if (arr == null || pos == null) return null;
    byte[] res = new byte[pos.length];
    for (int i = 0; i < pos.length; i++) {
      res[i] = arr.length > pos[i] ? arr[pos[i]] : 0;
    }
    return res;
  }

  public static byte[] extractBytes(byte[] src, byte... p) {
    byte[] r = null;
    for (int i = p.length - 1; i >= 0; i--) {
      if (p[i] == 0) r = new byte[i + 1];
    }

    assert r != null;
    System.arraycopy(src, 0, r, 0, r.length - 1 + 1);
    return r;
  }

  // region Basics
  public static byte[][] extBytes(byte[][] rsc, int[] pos) {
    byte[][] res = new byte[rsc.length][pos.length];
    for (int i = 0; i < rsc.length; i++) {
      for (int j = 0; j < pos.length; j++) {
        res[i][j] = rsc[i][pos[j]];
      }
    }
    return res;
  }

  // keys must be sorted, meaning same byte at depth must be consecutive!
  private static List<List<byte[]>> splitAt(List<byte[]> keys, int depth) {
    List<List<byte[]>> res = new ArrayList<>();
    byte cur = 0;
    List<byte[]> keysAtCur = new ArrayList<>();
    for (int i = 0; i < keys.size(); i++) {
      if (keys.get(i).length <= depth) {
        continue;
      }

      if (keys.get(i)[depth] == cur) {
        keysAtCur.add(keys.get(i));
      } else {
        if (!keysAtCur.isEmpty()) {
          res.add(keysAtCur);
          keysAtCur = new ArrayList<>();
        }

        cur = keys.get(i)[depth];
        keysAtCur.add(keys.get(i));
      }
    }

    res.add(keysAtCur);
    return res;
  }

  public static byte[] assembleKeyByBatch(byte[] bk, byte[] rk, int[] pos) {
    assert pos.length == bk.length : "length equality";
    for (int i = 1; i < pos.length; i++) {
      assert pos[i] >= pos[i - 1] && pos[i - 1] >= 0 : "must be sorted";
    }

    final byte[] res = new byte[rk.length + bk.length];

    int bkIndex = 0, rkIndex = 0, resIndex = 0;

    for (int p : pos) {
      int len = p - rkIndex - bkIndex;
      if (len > 0) {
        System.arraycopy(rk, rkIndex, res, resIndex, len);
        resIndex += len;
        rkIndex += len;
      }

      res[resIndex++] = bk[bkIndex++];
    }

    if (rkIndex < rk.length) {
      System.arraycopy(rk, rkIndex, res, resIndex, rk.length - rkIndex);
    }

    return res;
  }

  // endregion

  // region Parallel
  public static byte[][] parallelExtBytes(byte[][] rsc, int[] pos) {
    byte[][] res = new byte[rsc.length][];

    Arrays.parallelSetAll(
        res,
        i -> {
          byte[] extracted = new byte[pos.length];
          byte[] row = rsc[i];
          for (int j = 0; j < pos.length; j++) {
            extracted[j] = row[pos[j]];
          }
          return extracted;
        });
    return res;
  }

  public static int getValidBrPosNum(int keyLen, int[] brPos) {
    for (int j = 0; j < brPos.length; j++) {
      if (brPos[j] >= keyLen) return j;
    }
    return brPos.length;
  }

  public static byte[] setBytesByPosNoCheck(byte[] res, byte[] src, int[] pos) {
    if (src == null) return res;

    for (int i = 0; i < pos.length && i < src.length && res.length > pos[i]; i++) {
      res[pos[i]] = src[i];
    }

    return res;
  }

  public static Map<List<Byte>, List<byte[]>> findPrefixes(byte[][] arrays, int prefixLength) {
    return Arrays.stream(arrays)
        .parallel()
        .filter(arr -> arr.length >= prefixLength)
        .collect(
            Collectors.groupingByConcurrent(
                arr -> getPrefix(arr, prefixLength), Collectors.toList()));
  }

  private static List<Byte> getPrefix(byte[] array, int prefixLength) {
    List<Byte> prefix = new ArrayList<>(prefixLength);
    for (int i = 0; i < prefixLength; i++) {
      prefix.add(array[i]);
    }
    return prefix;
  }

  public static int findLCPLength(byte[][] arrays) {
    return findLCPLength(arrays, 0);
  }

  // LCP for Longest Common Prefix
  public static int findLCPLength(byte[][] arrays, int start) {
    if (arrays == null || arrays.length == 0) {
      return 0;
    }

    if (arrays.length == 1) return arrays[0].length;

    int minLength = Arrays.stream(arrays).mapToInt(arr -> arr.length).min().orElse(0);

    if (minLength < start) {
      return 0;
    }

    int prefixLength = 0;

    for (int i = start; i < minLength; i++) {
      byte currentByte = arrays[0][i];
      boolean allMatch = true;

      for (int j = 1; j < arrays.length; j++) {
        if (arrays[j][i] != currentByte) {
          allMatch = false;
          break;
        }
      }

      if (allMatch) {
        prefixLength++;
      } else {
        break;
      }
    }

    return prefixLength;
  }

  // endregion

  public static void main(String[] args) {
    byte[] t = new byte[] {1, 2, 88, 4};
    int[] ti = new int[] {1, 3};
    System.out.println(Arrays.toString(findIntervals(ti)));
    System.out.println(Arrays.toString(int2BytesNoTrailing(bytes2Int(t))));

    t = new byte[] {1, 2};
    System.out.println(Arrays.toString(int2BytesNoTrailing(bytes2Int(t))));
    t = new byte[] {8};
    System.out.println(Arrays.toString(int2BytesNoTrailing(bytes2Int(t))));
    t = new byte[] {1, 2, 3, 4};
    System.out.println(Arrays.toString(int2BytesNoTrailing(bytes2Int(t))));
  }

  // for assembler
  public static void main2(String[] args) {
    byte[] bk = new byte[] {1, 2, 3, 4, 11, 11, 11, 11};
    byte[] rk = new byte[] {5, 6, 7, 8, 44, 78, 33};
    int[] pos = new int[] {1, 2, 3, 4, 8, 9, 10, 11};
    byte[] res = assembleKeyByBatch(bk, rk, pos);
    System.out.println(Arrays.toString(res));
  }

  // on real dataset
  public static void main1(String[] args) throws Exception {
    String[] test =
        new String[] {
          "0110100101",
          "0110100110",
          "0110101010",
          "0110101011",
          "0111010110",
          "0111101001",
          "0111101011"
        };

    List<String> keysList =
        Arrays.asList(
            "elderberry",
            "fig",
            "grape",
            "guava",
            "kiwi",
            "kumquat",
            "lemon",
            "lime",
            "quince",
            "raspberry",
            "raspbersy",
            "strawberry",
            "tangerine",
            "apple",
            "applef",
            "apricot",
            "banana",
            "bandana",
            "cherry",
            "charming",
            "date",
            "dragonfruit",
            "yellowfruit",
            "zucchini",
            "mango",
            "nectarine",
            "orange",
            "papaya",
            "ugli",
            "vanilla",
            "watermelon",
            "xigua");

    byte[][] toSort = ByteEncode.strings2ByteArrays(keysList);

    List<ValuedPrefixArray> vpa = evaluatePrefixes(keysList);

    Arrays.sort(toSort, BYTE_ARRAY_COMPARATOR);
    String[] sortRes = ByteEncode.bytes2Strings(toSort);
    int bsRes =
        Arrays.binarySearch(
            toSort, "apple".getBytes(StandardCharsets.UTF_8), BYTE_ARRAY_COMPARATOR);

    PathTxtLoader loader = new PathTxtLoader(PathTxtLoader.FILE_PATH);
    List<String> kl = loader.getAllLines();
  }

  // region Export

  @Deprecated
  /** refers to {@link InfixGroup#groupByInfix} */
  public static List<ValuedPrefixArray> groupPrefixes(byte[][] keys, int start, int grpLen) {
    // group keys by the first byte
    Map<List<Byte>, List<byte[]>> classfier =
        Arrays.stream(keys)
            .parallel()
            .filter(arr -> arr.length >= start + 1) // prefixed key filtered
            .collect(
                Collectors.groupingByConcurrent(
                    arr -> {
                      List<Byte> prefix = new ArrayList<>(grpLen);
                      for (int i = start; i < grpLen + start; i++) {
                        prefix.add(arr[i]);
                      }
                      return prefix;
                    },
                    Collectors.toList()));

    // evaluate the effect of merging for each group
    List<ValuedPrefixArray> vpaList =
        classfier.values().stream()
            .map(e -> new ValuedPrefixArray(e.toArray(new byte[0][0]), start))
            .sorted(
                Comparator.comparingInt((ValuedPrefixArray obj) -> obj.len)
                    .thenComparingInt(obj -> obj.prd)
                    .reversed())
            .collect(Collectors.toList());
    return vpaList;
  }

  // endregion

  // region Utils

  public static List<ValuedPrefixArray> evaluatePrefixes(List<String> keys) {
    byte[][] toSort = ByteEncode.strings2ByteArrays(keys);

    // group keys by the first byte
    Map<List<Byte>, List<byte[]>> classfier = findPrefixes(toSort, 1);

    // evaluate the effect of merging for each group
    List<ValuedPrefixArray> vpaList =
        classfier.values().stream()
            .filter(v -> v.size() > 1) /* group with single key shall be filtered */
            .map(e -> new ValuedPrefixArray(e.toArray(new byte[0][0])))
            .sorted(
                Comparator.comparingInt((ValuedPrefixArray obj) -> obj.len)
                    .thenComparingInt(obj -> obj.prd)
                    .reversed())
            .collect(Collectors.toList());
    return vpaList;
  }

  private static final Comparator<byte[]> BYTE_ARRAY_COMPARATOR =
      (a, b) -> {
        int cmp;
        for (int i = 0; i < a.length && i < b.length; i++) {
          cmp = Byte.compare(a[i], b[i]);
          if (cmp != 0) {
            return cmp;
          }
        }
        return Integer.compare(a.length, b.length);
      };

  public static ICNode chooseCNodes(int[] pos, int fo) {
    int span = pos.length;
    switch (span) {
      case 8:
      case 7:
      case 6:
      case 5:
        if (fo > 32) return new HCNode8(pos);
        else return new SCNode8(pos);
      case 4:
      case 3:
        if (fo > 32) return new HCNode4(pos);
        else return new SCNode4(pos);
      case 2:
        if (fo > 32) return new HCNode2(pos);
        else return new SCNode2(pos);
      case 1:
        if (fo > 48) {
          CNode1F256 node = new CNode1F256();
          node.pos = pos[0];
          return node;
        } else if (fo > 32) {
          CNode1F48 node = new CNode1F48();
          node.pos = pos[0];
          return node;
        } else {
          CNode1FBS node = new CNode1FBS();
          node.pos = pos[0];
          return node;
        }
      default:
        throw new UnsupportedOperationException();
    }
  }

  // endregion

  public static class ValuedPrefixArray {
    // the prd (product) indicates the profit to merge this prefix
    public int len, prd;
    public byte[][] bytes;

    public ValuedPrefixArray(byte[][] b) {
      len = findLCPLength(b);
      bytes = b;
      prd = len * b.length;
    }

    public ValuedPrefixArray(byte[][] b, int start) {
      len = b.length == 0 ? 0 : findLCPLength(b, start);
      bytes = b;
      prd = len * b.length;
    }

    @Override
    public String toString() {
      return String.format(
          "%d x %d : %s",
          bytes.length, len, new String(bytes[0], StandardCharsets.UTF_8).substring(0, len));
    }
  }
}
