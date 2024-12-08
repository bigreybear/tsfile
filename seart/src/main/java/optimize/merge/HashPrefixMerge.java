package optimize.merge;

import static optimize.MergePrefix.partialNotMerge;
import static optimize.MergePrefix.partialToMerge;
import static optimize.nodes.cdm.CNodeHelper.findLCPLength;
import static optimize.util.InfixGroup.groupByInfix;
import static optimize.nodes.cdm.CNodeHelper.groupPrefixes;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import optimize.Evaluator;
import optimize.MergePrefix;
import optimize.nodes.INode;
import optimize.nodes.cdm.CNodeHelper;
import optimize.nodes.hash.HNodeV2;

public class HashPrefixMerge {

  /**
   * An experimental function, may be less read-friendly comparing to the V1 function.
   *
   * @param logicalChild wrapping oriNode for its getChild function
   */
  public static INode recNextMergeOnHashV2(
      Function<byte[], INode> logicalChild,
      byte[][] keys,
      int preLen,
      Evaluator.MergeStrategy ms,
      Evaluator.MapType mt,
      int height) {

    final int len = findLCPLength(keys, preLen);

    if (len == 0 && ms.equals(Evaluator.MergeStrategy.SIMPLE)) {
        final HNodeV2 repNode = initHashNodeWithPartialKey(keys[0], preLen, len);
        for (byte[] key : keys) {
          repNode.add(key, logicalChild.apply(key));
        }
      return repNode;
    }

    final HNodeV2 repNode = initHashNodeWithPartialKey(keys[0], preLen, len);
    List<byte[]> prefixedKeys =
        Arrays.stream(keys).filter(e -> e.length == len + preLen).collect(Collectors.toList());
    if (!prefixedKeys.isEmpty()) {
      repNode.add(new byte[0], logicalChild.apply(prefixedKeys.get(0)));
    }

    if (ms.equals(Evaluator.MergeStrategy.SIMPLE)) {
      // all keys longer than prefix will be added AS IS
      List<byte[]> longerKeys =
          Arrays.stream(keys).filter(e -> e.length > len + preLen).collect(Collectors.toList());
      for (byte[] nk : longerKeys) {
        repNode.add(
            Arrays.copyOfRange(nk, preLen + len, nk.length),
            logicalChild.apply(nk));
      }
      MergePrefix.occ.incrementAndGet();
      MergePrefix.ttlLen.addAndGet(longerKeys.size() * len);
      return repNode;
    }

    // for partial or all, recursively group keys after prefix and merge again
    List<CNodeHelper.ValuedPrefixArray> groupedPrefix = groupPrefixes(keys, len + preLen, 1);
    boolean toMergeAndExpand;
    for (CNodeHelper.ValuedPrefixArray vpa : groupedPrefix) {
      toMergeAndExpand = false;

      if (ms.equals(Evaluator.MergeStrategy.FULL) && vpa.bytes.length> 1) {
        toMergeAndExpand = true;
      }

      if (ms.equals(Evaluator.MergeStrategy.PARTIAL)) {
        if (Evaluator.evaluateMerge(vpa, preLen, height, keys.length, mt)) {
          partialToMerge.incrementAndGet();
          toMergeAndExpand = true;
        } else {
          partialNotMerge.incrementAndGet();
        }
      }

      if (toMergeAndExpand) {
        // when to execute: Full merge or evaluated worthy, and shared by more than ONE key
        MergePrefix.occ.incrementAndGet();
        MergePrefix.ttlLen.addAndGet(vpa.prd);
        final INode recNode =
            recNextMergeOnHashV2(
                logicalChild,
                vpa.bytes,
                len + preLen + 1,
                ms,
                mt,
                height);
        // add the node generated in rec to the current node
        repNode.add(
            Arrays.copyOfRange(vpa.bytes[0],len + preLen,len+ preLen+ 1),
            recNode);
      } else {
        // go-through to the no-branching child
        for (byte[] k : vpa.bytes) {
          repNode.add(Arrays.copyOfRange(k, preLen + len, k.length), logicalChild.apply(k));
        }
      }
    }
    return repNode;
  }

  public static HNodeV2 initHashNodeWithPartialKey(
      byte[] key,
      int preLen,
      int len) {
    HNodeV2 res = new HNodeV2();
    res.pk = len == 0 ? null : Arrays.copyOfRange(key, preLen, len + preLen);
    return res;
  }
}
