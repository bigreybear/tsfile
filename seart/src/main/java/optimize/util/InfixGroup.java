package optimize.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import optimize.nodes.cdm.CNodeHelper;

// return type of infix group
public class InfixGroup {
  List<Integer> brPos;
  Map<ByteArray, List<byte[]>> infixMap;
  int maxKeyLen = -1;

  private InfixGroup(int[] bp, Map<ByteArray, List<byte[]>> im) {
    brPos = Arrays.stream(bp).boxed().collect(Collectors.toList());

    infixMap = new TreeMap<>();
    ByteArray key;
    for (Map.Entry<ByteArray, List<byte[]>> entry : im.entrySet()) {
      key = new ByteArray(ArrayHelper.removeTrailingZeros(entry.getKey().getVal()));
      maxKeyLen = Math.max(key.val.length, maxKeyLen);
      infixMap.put(
          key,
          entry.getValue());
    }
  }

  public static InfixGroup groupByInfix(final byte[][] keys, final int limit, final int start) {
    return groupByInfix(Arrays.asList(keys), limit, start);
  }

  /**
   * Start from the offset and stop when the first branching position is occurred.
   * @return the
   */
  public static InfixGroup groupByInfix(final List<byte[]> keys, final int off) {
    return groupByInfix(keys, 1, off);
  }

  /**
   * Well-defined, which is implemented with no hurry :).
   */
  public static InfixGroup groupByInfix(final List<byte[]> keys, final int limit, final int start) {
    List<Integer> positions = new ArrayList<>();
    Map<ByteArray, List<byte[]>> infixKeyMap = new ConcurrentHashMap<>();

    infixKeyMap.put(new ByteArray(), keys);
    int depth = start;
    OptionalInt maxLen = keys.stream().mapToInt(e -> e.length).max();
    boolean isBranch;
    while (positions.size() < limit && !infixKeyMap.isEmpty() && depth < maxLen.getAsInt()) {
      int thisDepth = depth;

      // when split, branching keys immediately obtain its brKeys, while non-brs obtain after others
      // finished.
      Map<ByteArray, List<byte[]>> branchingKeys = new ConcurrentHashMap<>();
      Map<ByteArray, List<byte[]>> nonBranchingKeys = new ConcurrentHashMap<>();

      isBranch = parallelCheckSplit(
          infixKeyMap,
          branchingKeys,
          nonBranchingKeys,
          thisDepth
      );

      if (isBranch) {
        positions.add(depth);
        // improve: choose the smaller one as the base
        infixKeyMap = branchingKeys;
        for (Map.Entry<ByteArray, List<byte[]>> nbe : nonBranchingKeys.entrySet()) {
          // all keys in this entry must agree on thisDepth
          byte[] k1 = nbe.getValue().get(0);
          infixKeyMap.put(
              new ByteArray(nbe.getKey(), k1.length > thisDepth ? k1[thisDepth] : 0),
              nbe.getValue());
        }
      } else {
        // no key split at this depth
        infixKeyMap = nonBranchingKeys;
      }
      depth++;
    }
    return new InfixGroup(positions.stream().mapToInt(i -> i).sorted().toArray(), infixKeyMap);
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
        map.computeIfAbsent((byte) 0, k -> new ArrayList<>()).add(key);
        continue;
      }

      byte curByte = key[depth];
      map.computeIfAbsent(curByte, k -> new ArrayList<>()).add(key);
    }

    return map;
  }

  public void findNextBranch() {
    int oriPosLen = brPos.size();
    int dep = brPos.get(oriPosLen- 1);
    boolean foundBranch;
    while (true) {
      // imitate groupByInfix method
      int thisDepth = dep;
      Map<ByteArray, List<byte[]>> branchingKeys = new ConcurrentHashMap<>();
      Map<ByteArray, List<byte[]>> nonBranchingKeys = new ConcurrentHashMap<>();

      foundBranch = parallelCheckSplit(
          infixMap,
          branchingKeys,
          nonBranchingKeys,
          dep
      );

      if (foundBranch) {
        brPos.add(dep);
        infixMap = branchingKeys;
        for (Map.Entry<ByteArray, List<byte[]>> nbe : nonBranchingKeys.entrySet()) {
          byte[] k1 = nbe.getValue().get(0);
          infixMap.put(
              new ByteArray(nbe.getKey(), k1.length > thisDepth ? k1[thisDepth] : 0),
              nbe.getValue());
        }
        break;
      }

      infixMap = nonBranchingKeys;
      dep++;
    }
    infixMap = new TreeMap<>(infixMap);
  }

  private static boolean parallelCheckSplit(final Map<ByteArray, List<byte[]>> oriMap,
                                         final Map<ByteArray, List<byte[]>> brcMap,
                                         final Map<ByteArray, List<byte[]>> nbrMap,
                                         final int dep) {
    AtomicBoolean flag = new AtomicBoolean(false);
    oriMap.entrySet().parallelStream()
        .forEach(
            e -> {
              Map<Byte, List<byte[]>> res = splitAt(e.getValue(), dep);
              if (res.size() > 1) {
                flag.set(true);
                for (Map.Entry<Byte, List<byte[]>> resEnt : res.entrySet()) {
                  brcMap.put(
                      new ByteArray(e.getKey(), resEnt.getKey()), resEnt.getValue());
                }
              } else {
                // not branching, just key it as is, for further completion
                nbrMap.put(e.getKey(), e.getValue());
              }
            });
    return flag.get();
  }

  public List<byte[]> getCompleteKeys(byte[] brKey) {
    return infixMap.get(new ByteArray(ArrayHelper.removeTrailingZeros(brKey)));
  }

  public int[] getBranchingPos() {
    return brPos.stream().mapToInt(Integer::intValue).toArray();
  }

  public int[] sortedBrKeys() {
    if (brPos.size() > 4) throw new RuntimeException("More than 4 branching positions.");
    return infixMap.keySet().stream()
        .mapToInt(ba -> CNodeHelper.bytes2Int(ba.getVal()))
        .sorted()
        .toArray();
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

    builder.append("\n");
    builder.append("ctn: {\n");

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
}
