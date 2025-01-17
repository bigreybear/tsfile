package optimize.util;

import static optimize.nodes.cdm.ByteEncode.bytes2Int;
import static optimize.nodes.cdm.ByteEncode.bytes2Long;
import static optimize.nodes.cdm.ByteEncode.bytes2Short;
import static optimize.nodes.cdm.ByteEncode.strings2ByteArrays;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

// return type of infix group
public class InfixGroup {
  final List<Integer> brPos;

  // Note(zx) keys may be padded with trailing 0s. Harmless to perf but easier to revert.
  final Map<ByteArray, List<byte[]>> infixMap;

  // initial states, and partial key is implied by offset and the first in brPos
  final int maxKeyLen, offset;

  final byte[] commonPrefix;

  private InfixGroup(List<Integer> bp, Map<ByteArray, List<byte[]>> im, int ofs) {
    brPos = bp;

    infixMap = new TreeMap<>();
    ByteArray key;
    int mkl = -1;
    byte[] anyKey = null;
    for (Map.Entry<ByteArray, List<byte[]>> entry : im.entrySet()) {
      key = new ByteArray(entry.getKey().getVal());
      for (byte[] ck : entry.getValue()) {
        mkl = Math.max(mkl, ck.length);
      }
      if (anyKey == null) anyKey = entry.getValue().get(0);
      infixMap.put(key, entry.getValue());
    }

    maxKeyLen = mkl;
    offset = ofs;
    commonPrefix = (ofs < brPos.get(0) && anyKey != null)
        ? Arrays.copyOfRange(anyKey, ofs, brPos.get(0)) : null;
  }

  public static InfixGroup groupByInfix(final byte[][] keys, final int limit, final int start) {
    return groupByInfix(Arrays.asList(keys), limit, start);
  }

  public static InfixGroup groupByInfix(final byte[][] keys, final int off) {
    return groupByInfix(keys, 1, off);
  }

  /**
   * Start from the offset and stop when the first branching position is occurred.
   *
   * @return the
   */
  public static InfixGroup groupByInfix(final List<byte[]> keys, final int off) {
    return groupByInfix(keys, 1, off);
  }

  /** Well-defined, which is implemented with no hurry :). Actual constructor method. */
  public static InfixGroup groupByInfix(final List<byte[]> keys, final int limit, final int start) {
    List<Integer> positions = new ArrayList<>();

    Map<ByteArray, List<byte[]>> infixKeyMap = new ConcurrentHashMap<>(),
        brKeys = new ConcurrentHashMap<>(),
        nonBrKeys = new ConcurrentHashMap<>(),
        tempRef; // make sure only 3 map instances are created

    infixKeyMap.put(new ByteArray(), keys);
    int depth = start;
    OptionalInt maxLen = keys.stream().mapToInt(e -> e.length).max();
    while (positions.size() < limit && !infixKeyMap.isEmpty() && depth < maxLen.getAsInt()) {
      // when split, branching keys immediately obtain its brKeys, while non-brs obtain after others
      // finished.

      parallelCheckSplit(infixKeyMap, brKeys, nonBrKeys, depth);

      if (!brKeys.isEmpty()) {
        positions.add(depth);
        // improve: choose the smaller one as the base
        tempRef = infixKeyMap;
        infixKeyMap = brKeys;
        for (Map.Entry<ByteArray, List<byte[]>> nbe : nonBrKeys.entrySet()) {
          // all keys in this entry must agree on thisDepth
          byte[] k1 = nbe.getValue().get(0);
          infixKeyMap.put(
              new ByteArray(nbe.getKey(), k1.length > depth ? k1[depth] : 0), nbe.getValue());
        }

        brKeys = tempRef;
        // the two map instances could be reused other than garbage collected.
        brKeys.clear();
        nonBrKeys.clear();
      } else {
        // no branching, swap infix and nonBrKeys and clear content in coming nonBrKeys
        tempRef = infixKeyMap;
        infixKeyMap = nonBrKeys;
        nonBrKeys = tempRef;
        nonBrKeys.clear();
      }
      depth++;
    }
    return new InfixGroup(positions, infixKeyMap, start);
  }

  /**
   * Only split at designated position. <br>
   * Key method being called multiple times.
   *
   * @return for each entry <b, List<k>>, all k has value b at depth
   */
  private static Map<Byte, List<byte[]>> splitAt(List<byte[]> keys, int depth) {
    if (keys.isEmpty()) {
      return new HashMap<>();
    }

    Map<Byte, List<byte[]>> map = new HashMap<>();
    for (byte[] key : keys) {
      // keys not long enough will be grouped as '0'
      if (key.length <= depth) {
        // Note(zx) a byte 0 is padded for shorter keys
        map.computeIfAbsent((byte) 0, k -> new ArrayList<>()).add(key);
        continue;
      }

      byte curByte = key[depth];
      map.computeIfAbsent(curByte, k -> new ArrayList<>()).add(key);
    }

    return map;
  }

  // return true if next branch found otherwise false
  public boolean findNextBranch() {
    final int oriPosLen = brPos.size();
    int dep = brPos.get(oriPosLen - 1) + 1;
    if (dep == maxKeyLen) return false; // no more split

    Map<ByteArray, List<byte[]>> nextRun = new ConcurrentHashMap<>(infixMap),
        brcKeys = new ConcurrentHashMap<>(),
        nonBrcKeys = new ConcurrentHashMap<>(),
        refChange;
    while (dep < maxKeyLen) {
      parallelCheckSplit(nextRun, brcKeys, nonBrcKeys, dep);

      if (!brcKeys.isEmpty()) {
        brPos.add(dep);
        nextRun = brcKeys;
        // there IS a branch, and those non-branching shall fill the corresponding byte to provide
        // branch key
        for (Map.Entry<ByteArray, List<byte[]>> nbe : nonBrcKeys.entrySet()) {
          // k1 and other elements are identical on thisDepth
          byte[] k1 = nbe.getValue().get(0);
          nextRun.put(
              new ByteArray(nbe.getKey(), k1.length > dep ? k1[dep] : 0),
              nbe.getValue());
        }
        break;
      }

      // swap nextRun and nonBrcKeys
      refChange = nextRun;
      nextRun = nonBrcKeys;
      nonBrcKeys = refChange;

      // brKeys is already emtpy
      nonBrcKeys.clear();
      dep++;
    }
    // infixMap = new TreeMap<>(infixMap);
    infixMap.clear();
    infixMap.putAll(nextRun);
    return oriPosLen != brPos.size();
  }

  public boolean revertSplit() {
    if (brPos.size() == 1) return false;

    System.out.println("Valid revert.");

    final int nps = brPos.size() - 1;
    Map<ByteArray, List<byte[]>> temp = new TreeMap<>();
    for (Map.Entry<ByteArray, List<byte[]>> entry : infixMap.entrySet()) {
      temp.computeIfAbsent(entry.getKey().getSlice(0, nps), k -> new ArrayList<>())
          .addAll(entry.getValue());
    }
    infixMap.clear();
    infixMap.putAll(temp);
    brPos.remove(nps);
    return true;
  }

  // check result with brcMap.size()
  private static void parallelCheckSplit(
      final Map<ByteArray, List<byte[]>> oriMap,
      final Map<ByteArray, List<byte[]>> brcMap,
      final Map<ByteArray, List<byte[]>> nbrMap,
      final int dep) {
    oriMap.entrySet().parallelStream()
        .forEach(
            e -> {
              Map<Byte, List<byte[]>> res = splitAt(e.getValue(), dep);
              if (res.size() > 1) {
                for (Map.Entry<Byte, List<byte[]>> resEnt : res.entrySet()) {
                  brcMap.put(new ByteArray(e.getKey(), resEnt.getKey()), resEnt.getValue());
                }
              } else {
                // not branching, just keep it as is, for further completion
                // since, the byte in #dep could be padded or dropped, depends on other entries
                nbrMap.put(e.getKey(), e.getValue());
              }
            });
  }

  public List<byte[]> getCompleteKeys(byte[] brKey) {
    // Answer for why remove trailing 0s before the call:
    //  the brKey could be 0-trailing for 2 case:
    //    1) other branches are longer than the passing one(trailed by padded);
    //    2) the branching incurred in the last bytes of the key(trailed by decoded).
    //  Outside the InfixGroup, it cannot be told which is true, and the first case shall keep
    //  the trailing while only the other one shall remove.
    // This method is only called when constructing so trivial to query perf.
    return infixMap.get(
        new ByteArray(
            brPos.size() == brKey.length ? brKey : Arrays.copyOfRange(brKey, 0, brPos.size())));
  }

  public int[] getBranchingPos() {
    return brPos.stream().mapToInt(Integer::intValue).toArray();
  }

  public byte[] sortedByteBranchKeys() {
    if (brPos.size() > 1) throw new RuntimeException("More than 1 branching positions.");
    byte[] r = new byte[infixMap.size()];
    int idx = 0;
    for (ByteArray k : infixMap.keySet()) r[idx++] = k.val[0];
    Arrays.sort(r);
    return r;
  }

  // Note(zx) all sorting are ascending
  public short[] sortedShortBranchKeys() {
    // sorted as signed-short so it conforms to Arrays.binarySearch
    if (brPos.size() > 2) throw new RuntimeException("More than 2 branching positions.");
    short[] r = new short[infixMap.size()];
    int idx = 0;
    for (ByteArray k : infixMap.keySet()) r[idx++] = bytes2Short(k.getVal());
    Arrays.sort(r);
    return r;
  }

  public int[] sortedIntBranchKeys() {
    if (brPos.size() > 4) throw new RuntimeException("More than 4 branching positions.");
    int[] r = new int[infixMap.size()];
    int idx = 0;
    for (ByteArray k : infixMap.keySet()) r[idx++] = bytes2Int(k.getVal());
    Arrays.sort(r);
    return r;
  }

  public long[] sortedLongBranchKeys() {
    if (brPos.size() > 8) throw new RuntimeException("More than 8 branching positions.");
    long[] r = new long[infixMap.size()];
    int idx = 0;
    for (ByteArray k : infixMap.keySet()) r[idx++] = bytes2Long(k.getVal());
    Arrays.sort(r);
    return r;
  }

  public byte[][] sortedBrKeyBytes() {
    byte[][] keys = infixMap.keySet().stream().map(ByteArray::getVal).toArray(byte[][]::new);
    Arrays.sort(keys, Arrays::compare);
    return keys;
  }

  public Map<ByteArray, List<byte[]>> getInfixMap() {
    return infixMap;
  }

  public int countBranches() {
    return infixMap.size();
  }

  public int countPositions() {
    return brPos.size();
  }

  public byte[] getCommonPrefix() {
    return commonPrefix;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("Positions: [");
    for (int i = 0; i < brPos.size(); i++) {
      builder.append(brPos.get(i));
      if (i == brPos.size() - 1) {
        builder.append("] ");
      } else {
        builder.append(",");
      }
    }

    builder.append(String.format(" Branches num: %d", infixMap.size()));
    builder.append("\n");
    builder.append("content: {\n");

    List<Map.Entry<ByteArray, List<byte[]>>> lst = new ArrayList<>(infixMap.entrySet());
    for (int i = 0; i < lst.size(); i++) {
      builder.append("\t");
      builder.append(Arrays.toString(lst.get(i).getKey().val));
      builder.append(": ");
      builder.append(lst.get(i).getValue().size());
      builder.append("\n");
    }
    builder.append("}");
    return builder.toString();
  }

  public static void main(String[] args) {
    List<String> keysList = Arrays.asList("aaa", "aaa", "aabdd", "aabdc", "aac");

    InfixGroup ig = groupByInfix(strings2ByteArrays(keysList), 0);
    boolean f;
    System.out.println(ig);
    f = ig.findNextBranch();
    System.out.println(f);
    System.out.println(ig);
    f = ig.findNextBranch();
    System.out.println(f);
    System.out.println(ig);
    System.out.println(ig.findNextBranch());
    System.out.println(ig);
    System.out.println(ig.revertSplit());
    System.out.println(ig);
  }
}
