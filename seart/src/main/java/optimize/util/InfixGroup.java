package optimize.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import optimize.nodes.cdm.CNodeHelper;

// return type of infix group
public class InfixGroup {
  int[] brPos;
  Map<ByteArray, List<byte[]>> infixMap;

  public InfixGroup(int[] bp, Map<ByteArray, List<byte[]>> im) {
    brPos = bp;

    infixMap = new HashMap<>();
    for (Map.Entry<ByteArray, List<byte[]>> entry : im.entrySet()) {
      infixMap.put(
          new ByteArray(ArrayHelper.removeTrailingZeros(entry.getKey().getVal())),
          entry.getValue());
    }
  }

  public static InfixGroup groupByInfix(final byte[][] keys, final int limit, final int start) {
    return groupByInfix(Arrays.asList(keys), limit, start);
  }

  /**
   * Well-defined, which is implemented with no hurry :). <br>
   * With this method, both {@linkplain CNodeHelper#groupPrefixes} and {@linkplain
   * InfixGroup#getBranchingPosParallel} gets Deprecated.
   */
  public static InfixGroup groupByInfix(final List<byte[]> keys, final int limit, final int start) {
    Set<Integer> positions = ConcurrentHashMap.newKeySet();
    Map<ByteArray, List<byte[]>> infixKeyMap = new ConcurrentHashMap<>();

    infixKeyMap.put(new ByteArray(), keys);
    int depth = start;
    OptionalInt maxLen = keys.stream().mapToInt(e -> e.length).max();
    while (positions.size() < limit && !infixKeyMap.isEmpty() && depth < maxLen.getAsInt()) {
      final int thisDepth = depth;

      // when split, branching keys immediately obtain its brKeys, while non-brs obtain after others
      // finished.
      final Map<ByteArray, List<byte[]>> branchingKeys = new ConcurrentHashMap<>();
      final Map<ByteArray, List<byte[]>> nonBranchingKeys = new ConcurrentHashMap<>();

      infixKeyMap.entrySet().parallelStream()
          .forEach(
              e -> {
                Map<Byte, List<byte[]>> res = parallelSplitAt(e.getValue(), thisDepth);
                if (res.size() > 1) {
                  positions.add(thisDepth);
                  // ByteArray tmpKey;
                  for (Map.Entry<Byte, List<byte[]>> resEnt : res.entrySet()) {
                    // tmpKey = new ByteArray(e.getKey(), resEnt.getKey());
                    branchingKeys.put(
                        new ByteArray(e.getKey(), resEnt.getKey()), resEnt.getValue());
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

  @Deprecated
  public static Set<Integer> getBranchingPosParallel(List<byte[]> byteKeys, int limit) {
    return getBranchingPosParallel(byteKeys, limit, 0);
  }

  @Deprecated
  // todo combine with grouping/classifier, avoid another stream/grouping
  //  make sure initial byteKys are identical on bytes before from.
  public static Set<Integer> getBranchingPosParallel(
      final List<byte[]> byteKeys, final int limit, final int from /* included */) {
    Set<Integer> positions = ConcurrentHashMap.newKeySet();
    Queue<List<byte[]>> cur = new ConcurrentLinkedQueue<>();

    cur.add(byteKeys);
    int depth = from;

    while (positions.size() < limit && !cur.isEmpty()) {
      final int thisDepth = depth;
      final Queue<List<byte[]>> tar = new ConcurrentLinkedQueue<>();
      cur.parallelStream()
          .forEach(
              group -> {
                Map<Byte, List<byte[]>> res = parallelSplitAt(group, thisDepth);
                if (res.size() > 1) {
                  positions.add(thisDepth);
                }

                res.values().stream().filter(e -> e.size() > 1).forEach(tar::add);
              });

      cur = tar;
      depth++;
    }

    return positions;
  }

  /**
   * Only split at designated position. <br>
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

  public List<byte[]> getCompleteKeys(byte[] brKey) {
    return infixMap.get(new ByteArray(ArrayHelper.removeTrailingZeros(brKey)));
  }

  public int[] getBranchingPos() {
    return brPos;
  }

  public int[] sortedBrKeys() {
    if (brPos.length > 4) throw new RuntimeException("More than 4 branching positions.");
    return infixMap.keySet().stream()
        .mapToInt(ba -> CNodeHelper.bytes2Int(ba.getVal()))
        .sorted()
        .toArray();
  }

  public byte[][] sortedBrKeyBytes() {
    byte[][] keys = infixMap.keySet().stream().map(i -> i.getVal()).toArray(byte[][]::new);
    Arrays.sort(keys, Arrays::compare);
    return keys;
  }

  public Map<ByteArray, List<byte[]>> getInfixMap() {
    return infixMap;
  }
}
