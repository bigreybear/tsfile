package optimize.nodes.cdm;


import loader.PathTxtLoader;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import static optimize.nodes.fdm.vfull.SEARTNode.ubyte;

public class CNodeHelper {
  public static final int POS_SIZE = 4;

  public static byte[] extractBytes(byte[] arr, int[] pos) {
    byte[] res = new byte[pos.length];
    for (int i = 0 ; i < pos.length; i++) {
      res[i] = arr.length > pos[i] ? arr[pos[i]] : 0;
    }
    return res;
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

  public static Set<Integer> getBranchingPositions(List<String> keys) {
    Collections.sort(keys);
    List<byte[]> byteKeys = keys.stream().map(s -> s.getBytes(StandardCharsets.UTF_8)).collect(Collectors.toList());
    Set<Integer> positions = new HashSet<>();
    List<List<byte[]>> cur = new ArrayList<>(), tar = new ArrayList<>(), temp /*only for swap*/;
    cur.add(byteKeys);
    int depth = 0;
    List<List<byte[]>> res;
    while (positions.size() < POS_SIZE && !cur.isEmpty()) {
      for (List<byte[]> group : cur) {
        res = splitAt(group, depth);
        if (res.size() > 1) {
          positions.add(depth);
        }

        // only group with more than 1 string can be considered to split
        tar.addAll(res.stream().filter(e -> e.size() > 1).collect(Collectors.toList()));
      }

      cur.clear();
      temp = cur;
      cur = tar;
      tar = temp;
      depth++;
    }

    return positions;
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
    assert pos.length == bk.length: "length equality";
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

    Arrays.parallelSetAll(res, i -> {
      byte[] extracted = new byte[pos.length];
      byte[] row = rsc[i];
      for (int j = 0; j < pos.length; j++) {
        extracted[j] = row[pos[j]];
      }
      return extracted;
    });
    return res;
  }

  public static Set<Integer> parallelGetBranchingPositions(List<String> keys, boolean posLimit) {
    return posLimit ? parallelGetBranchingPositions(keys, POS_SIZE) :
                      parallelGetBranchingPositions(keys, Integer.MAX_VALUE);
  }

  public static Set<Integer> parallelGetBranchingPositions(List<String> keys, int limit) {
    // todo remove the sort after thorough dev
    keys = keys.parallelStream().sorted().collect(Collectors.toList());

    List<byte[]> byteKeys = keys.parallelStream()
        .map(s -> s.getBytes(StandardCharsets.UTF_8))
        .collect(Collectors.toList());

    return getBranchingPosParallel(byteKeys, limit);
  }

  public static int[] complementaryBytePos(int preLen, int keyLen, int[] brPos) {
    int brBeforeKey = getValidBrPosNum(keyLen, brPos);
    int[] res = new int[keyLen - preLen - brBeforeKey];
    for (int i = 0, bpi = 0; i < res.length; ) {
      if (bpi < brBeforeKey && i + preLen + bpi == brPos[bpi] ) {
        bpi ++;
        continue;
      }

      res[i] = i + preLen + bpi;
      i++;
    }
    return res;
  }

  public static int getValidBrPosNum(int keyLen, int[] brPos) {
    for (int j = 0; j < brPos.length; j++) {
      if (brPos[j] >= keyLen) return j;
    }
    return brPos.length;
  }

  // return type of infix group
  public static class InfixGroup {
    int[] brPos;
    Map<ByteArray, List<byte[]>> infixMap;

    public InfixGroup(int[] bp, Map<ByteArray, List<byte[]>> im) {
      brPos = bp; infixMap = im;
    }

    public List<byte[]> getCompleteKeys(byte[] brKey) {
      return infixMap.get(new ByteArray(brKey));
    }

    public int[] getBranchingPos() {return brPos;}

    public int[] sortedBrKeys() {
      if (brPos.length > 4) throw new RuntimeException("More than 4 branching positions.");
      return infixMap.keySet().stream().mapToInt(ba->bytes2Int(ba.val)).sorted().toArray();
    }

    public byte[][] sortedBrKeyBytes() {
      byte[][] keys = infixMap.keySet().stream()
          .map(i -> i.val)
          .toArray(byte[][]::new);
      Arrays.sort(keys, Arrays::compare);
      return keys;
    }

    public Map<ByteArray, List<byte[]>> getInfixMap() {return infixMap;}
  }

  // substitute List<byte> for extreme performance
  public static class ByteArray {
    final byte[] val;

    public ByteArray() {val = null;}

    public ByteArray(ByteArray ori, byte add) {
      val = new byte[ori.val == null ? 1 : (ori.val.length + 1)];
      if (ori.val != null) System.arraycopy(ori.val, 0, val, 0, ori.val.length);
      val[val.length-1] = add;
    }

    public ByteArray(byte[] ba) {
      val = ba;
    }

    public byte[] getVal() {return val;}

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
  }

  /**
   * Well-defined, which is implemented with no hurry :). <br/>
   * With this method, both {@linkplain #groupPrefixes} and {@linkplain #getBranchingPosParallel}
   * gets Deprecated.
   * @param keys
   * @param limit
   * @param start
   *
   * @return
   */
  public static InfixGroup groupByInfix(final List<byte[]> keys,
                                        final int limit,
                                        final int start) {
    Set<Integer> positions = ConcurrentHashMap.newKeySet();
    Map<ByteArray, List<byte[]>> infixKeyMap = new ConcurrentHashMap<>();

    infixKeyMap.put(new ByteArray(), keys);
    int depth = start;
    OptionalInt maxLen = keys.stream().mapToInt(e->e.length).max();
    while (positions.size() < limit && !infixKeyMap.isEmpty() && depth < maxLen.getAsInt()) {
      final int thisDepth = depth;

      // when split, branching keys immediately obtain its brKeys, while non-brs obtain after others finished.
      final Map<ByteArray, List<byte[]>> branchingKeys = new ConcurrentHashMap<>();
      final Map<ByteArray, List<byte[]>> nonBranchingKeys = new ConcurrentHashMap<>();

      infixKeyMap.entrySet().parallelStream().forEach(e -> {
        Map<Byte, List<byte[]>> res = parallelSplitAt(e.getValue(), thisDepth);
        if (res.size() > 1) {
          positions.add(thisDepth);
          // ByteArray tmpKey;
          for (Map.Entry<Byte, List<byte[]>> resEnt : res.entrySet()) {
            // tmpKey = new ByteArray(e.getKey(), resEnt.getKey());
            branchingKeys.put(new ByteArray(e.getKey(), resEnt.getKey()), resEnt.getValue());
          }
        } else {
          // not branching, just key it as is, for further completion
          nonBranchingKeys.put(e.getKey(), e.getValue());
        }
      });

      if (positions.contains(depth)) {
        // improve: choose the smaller one as the base
        infixKeyMap = branchingKeys;
        for (Map.Entry<ByteArray, List<byte[]>> nbe : nonBranchingKeys.entrySet()) {
          // all keys in this entry must agree on thisDepth
          byte[] k1 = nbe.getValue().get(0);
          infixKeyMap.put(
              new ByteArray(
                  nbe.getKey(),
                  k1.length > thisDepth ? k1[thisDepth] : 0),
              nbe.getValue());
        }
      } else {
        // no key split at this depth
        infixKeyMap = nonBranchingKeys;
      }
      depth++;
    }
    return new InfixGroup(
        positions.stream().mapToInt(i->i).sorted().toArray(),
        infixKeyMap
    );
  }



  public static Set<Integer> getBranchingPosParallel(List<byte[]> byteKeys, int limit) {
    return getBranchingPosParallel(byteKeys, limit, 0);
  }

  // todo combine with grouping/classifier, avoid another stream/grouping
  //  make sure initial byteKys are identical on bytes before from.
  public static Set<Integer> getBranchingPosParallel(final List<byte[]> byteKeys,
                                                     final int limit,
                                                     final int from /* included */ ) {
    Set<Integer> positions = ConcurrentHashMap.newKeySet();
    Queue<List<byte[]>> cur = new ConcurrentLinkedQueue<>();

    cur.add(byteKeys);
    int depth = from;

    while (positions.size() < limit && !cur.isEmpty()) {
      final int thisDepth = depth;
      final Queue<List<byte[]>> tar = new ConcurrentLinkedQueue<>();
      cur.parallelStream().forEach(group -> {
        Map<Byte, List<byte[]>> res = parallelSplitAt(group, thisDepth);
        if (res.size() > 1) {
          positions.add(thisDepth);
        }

        res.values().stream()
            .filter(e -> e.size() > 1)
            .forEach(tar::add);
      });

      cur = tar;
      depth++;
    }

    return positions;
  }

  /**
   * Only split at designated position. <br/>
   * Key method being called multiple times.
   *
   * @return for each entry <b, List<k>>, all k has value b at depth
   */
  private static Map<Byte, List<byte[]>> parallelSplitAt(List<byte[]> keys, int depth) {
    if (keys.isEmpty()) {
      return new HashMap<>();
    }

    Map<Byte, List<byte[]>> map = new HashMap<>();
    for (byte[] key : keys) {
      // keys not long enough will be grouped as '0'
      if (key.length <= depth) {
        map.computeIfAbsent((byte) 0, k -> new ArrayList<>()).add(key);
        continue;
      }

      byte currentChar = key[depth];
      map.computeIfAbsent(currentChar, k -> new ArrayList<>()).add(key);
    }

    return map;
  }


  public static Map<List<Byte>, List<byte[]>> findPrefixes(byte[][] arrays, int prefixLength) {
    return Arrays.stream(arrays)
        .parallel()
        .filter(arr -> arr.length >= prefixLength)
        .collect(Collectors.groupingByConcurrent(
            arr -> getPrefix(arr, prefixLength),
            Collectors.toList()
        ));
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

    int minLength = Arrays.stream(arrays)
        .mapToInt(arr -> arr.length)
        .min()
        .orElse(0);

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
    byte[] t = new  byte[] {1,2,88,4};
    int[] ti = new int[] {1,3};
    System.out.println(Arrays.toString(findIntervals(ti)));
    System.out.println(Arrays.toString(int2BytesVarLen(bytes2Int(t))));

    t = new byte[] {1,2};
    System.out.println(Arrays.toString(int2BytesVarLen(bytes2Int(t))));
    t = new byte[] {8};
    System.out.println(Arrays.toString(int2BytesVarLen(bytes2Int(t))));
    t = new byte[] {1,2,3,4};
    System.out.println(Arrays.toString(int2BytesVarLen(bytes2Int(t))));


  }

  // for assembler
  public static void main2(String[] args) {
    byte[] bk = new byte[] {1, 2, 3, 4,11,11,11,11};
    byte[] rk = new byte[] {5,6,7,8,44,78,33};
    int[] pos = new int[] {1,2,3,4,8,9,10,11};
    byte[] res = assembleKeyByBatch(bk, rk, pos);
    System.out.println(Arrays.toString(res));
  }

  // on real dataset
  public static void main1(String[] args) throws Exception {
    String[] test = new String[] {
        "0110100101",
        "0110100110",
        "0110101010",
        "0110101011",
        "0111010110",
        "0111101001",
        "0111101011"
    };

    List<String> keysList = Arrays.asList(
        "elderberry", "fig", "grape", "guava",
        "kiwi", "kumquat", "lemon", "lime",
        "quince", "raspberry", "raspbersy", "strawberry", "tangerine",
        "apple", "applef", "apricot", "banana", "bandana",
        "cherry", "charming", "date", "dragonfruit",
        "yellowfruit", "zucchini",
        "mango", "nectarine", "orange", "papaya",
        "ugli", "vanilla", "watermelon", "xigua"
    );

    byte[][] toSort = strings2ByteArrays(keysList);

    List<ValuedPrefixArray> vpa = evaluatePrefixes(keysList);

    Arrays.sort(toSort, BYTE_ARRAY_COMPARATOR);
    String[] sortRes = bytes2Strings(toSort);
    int bsRes = Arrays.binarySearch(toSort, "apple".getBytes(StandardCharsets.UTF_8), BYTE_ARRAY_COMPARATOR);

    PathTxtLoader loader = new PathTxtLoader(PathTxtLoader.FILE_PATH);
    List<String> kl = loader.getAllLines();

    long time = System.nanoTime();
    Set<Integer> res = parallelGetBranchingPositions(kl, false);
    time = System.nanoTime() - time;
    System.out.println(time/1000000);
    System.out.println(res);

    byte[][] a = strings2ByteArrays(test);
    System.out.println(Arrays.toString(bytes2Strings(a)));

    byte[][] res2 = extBytes(a, new int[]{1,2,3,4});
    System.out.println(Arrays.toString(bytes2Strings(res2)));

  }


  // region Export

  @Deprecated
  // group keys by prefixes start from certain position.
  public static List<ValuedPrefixArray> groupPrefixes(byte[][] keys, int start, int grpLen) {
    // group keys by the first byte
    Map<List<Byte>, List<byte[]>> classfier = Arrays.stream(keys)
        .parallel()
        .filter(arr -> arr.length >= start + 1)  // prefixed key filtered
        .collect(Collectors.groupingByConcurrent(
            arr -> {
              List<Byte> prefix = new ArrayList<>(grpLen);
              for (int i = start; i < grpLen + start; i++) {
                prefix.add(arr[i]);
              }
              return prefix;
            },
            Collectors.toList()
        ));


    // evaluate the effect of merging for each group
    List<ValuedPrefixArray> vpaList = classfier.values()
        .stream()
        .map(e -> new ValuedPrefixArray(e.toArray(new byte[0][0]), start)).
        sorted(Comparator.comparingInt((ValuedPrefixArray obj) -> obj.len)
            .thenComparingInt(obj -> obj.prd).reversed())
        .collect(Collectors.toList());
    return vpaList;
  }

  // endregion

  // region Utils

  public static int[] findIntervals(byte[] pos) {
    int[] res = new int[pos.length];
    for (int i = 0; i < pos.length; i++) {
      res[i] = 0xff & pos[i];
    }
    return findIntervals(res);
  }

  public static int[] findIntervals(int[] pos) {
    if (pos == null || pos.length <= 1) return new int[0];

    int[] itvPos = new int[pos[pos.length - 1] - pos[0] - pos.length + 1];
    for (int idx = 0, k = 0;;) {
      // k records number in itvPos
      if (idx > pos.length - 2) break;  // shall not check last element

      if (pos[idx] + 1 != pos[idx + 1]) {
        for (int pi = pos[idx] + 1; pi < pos[idx+1]; pi++) {
          itvPos[k++] = pi;
        }
      }
      idx++;
    }
    return itvPos;
  }

  public static List<ValuedPrefixArray> evaluatePrefixes(List<String> keys) {
    byte[][] toSort = strings2ByteArrays(keys);

    // group keys by the first byte
    Map<List<Byte>, List<byte[]>> classfier = findPrefixes(toSort, 1);

    // evaluate the effect of merging for each group
    List<ValuedPrefixArray> vpaList = classfier.values()
        .stream()
        .filter(v -> v.size() > 1) /* group with single key shall be filtered */
        .map(e -> new ValuedPrefixArray(e.toArray(new byte[0][0]))).
        sorted(Comparator.comparingInt((ValuedPrefixArray obj) -> obj.len)
            .thenComparingInt(obj -> obj.prd).reversed())
        .collect(Collectors.toList());
    return vpaList;
  }

  public static Charset coding = StandardCharsets.UTF_8;

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
  private static String[] bytes2Strings(byte[][] b) {
    String[] r = new String[b.length];
    Arrays.parallelSetAll(r, i -> new String(b[i], coding));
    return r;
  }

  public static int bytes2IntLegacy(byte[] b) {
    // preceding bytes on higher bits
    // fixme and its wrong! cannot differ [1,2,3] and [0,1,2,3] as 0 on highest byte
    return (( (b.length >= 1 ? b[0] : 0) & 0xFF) << 24) |
        (((b.length >= 2 ? b[1] : 0) & 0xFF) << 16) |
        (((b.length >= 3 ? b[2] : 0) & 0xFF) << 8)  |
        ((b.length >= 4 ? b[3] : 0) & 0xFF);
  }

  public static int bytes2Int(byte[] b) {
    // put prior pos on lower bytes
    int len = b.length, r = 0;
    if (len > 4) throw new UnsupportedOperationException("5 or more bytes cannot encoded to a int.");
    for (int i = len - 1; i >= 0; i--) {
      r <<= 8;
      r |= ubyte(b[i]);
    }

    if (r==10524646)
      System.out.println("AAAA");
    return r;
  }

  // all bytes are processed as is, for fixed length
  public static byte[] int2BytesFixedLen(final int i, final int len) {
    byte[] b = new byte[4];
    int k = 0;
    for (; k < len ;) {
      b[k] = (byte) ((i >> (8 * k)) & 0xff);
      k++;
    }
    return Arrays.copyOfRange(b, 0, len);
  }

  /**
   * 0s are used as mark, only valid in the lowest byte (foremost byte previously)
   */
  public static byte[] int2BytesVarLen(final int i) {
    byte[] b = new byte[4];
    int k = 0;
    for (; k < 4 ;) {
      b[k] = (byte) ((i >> (8 * k)) & 0xff);
      if (b[k] == 0 && k != 0) break;  // 0 after any non-zero are ignored
      k++;
    }

    return Arrays.copyOfRange(b, 0, k);
  }

  private static final Comparator<byte[]> BYTE_ARRAY_COMPARATOR = (a, b) -> {
    int cmp;
    for (int i = 0; i < a.length && i < b.length; i++) {
      cmp = Byte.compare(a[i], b[i]);
      if (cmp != 0) {
        return cmp;
      }
    }
    return Integer.compare(a.length, b.length);
  };
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
      return String.format("%d x %d : %s", bytes.length, len,
          new String(bytes[0], StandardCharsets.UTF_8).substring(0, len));
    }
  }
}
