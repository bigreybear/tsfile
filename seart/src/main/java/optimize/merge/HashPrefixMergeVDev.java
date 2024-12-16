package optimize.merge;

import optimize.nodes.IMicroNode;
import optimize.nodes.cdm.CNodeHelper;
import optimize.nodes.hash.HNodeVDev;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static optimize.merge.MergePrefix.partialNotMerge;
import static optimize.merge.MergePrefix.partialToMerge;
import static optimize.nodes.cdm.CNodeHelper.findLCPLength;
import static optimize.nodes.cdm.CNodeHelper.groupPrefixes;

public class HashPrefixMergeVDev {

  /**
   * An experimental function, may be less read-friendly comparing to the V1 function.
   *
   * @param logicalChild wrapping oriNode for its getChild function
   */
  public static IMicroNode recNextMergeOnHashV2(
      Function<byte[], IMicroNode> logicalChild,
      byte[][] keys,
      int preLen,
      PrefixMergeStrategy ms,
      MapType mt,
      int height) {

    final int len = findLCPLength(keys, preLen);

    if (len == 0 && ms.equals(PrefixMergeStrategy.SIMPLE)) {
        final IMicroNode repNode = initHashNodeWithPartialKey(keys[0], preLen, len);
        for (byte[] key : keys) {
          repNode.setChild(key, logicalChild.apply(key));
        }
      return repNode;
    }

    final IMicroNode repNode = initHashNodeWithPartialKey(keys[0], preLen, len);
    List<byte[]> prefixedKeys =
        Arrays.stream(keys).filter(e -> e.length == len + preLen).collect(Collectors.toList());
    if (!prefixedKeys.isEmpty()) {
      repNode.setChild(new byte[0], logicalChild.apply(prefixedKeys.get(0)));
    }

    if (ms.equals(PrefixMergeStrategy.SIMPLE)) {
      // all keys longer than prefix will be added AS IS
      List<byte[]> longerKeys =
          Arrays.stream(keys).filter(e -> e.length > len + preLen).collect(Collectors.toList());
      for (byte[] nk : longerKeys) {
        repNode.setChild(
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
      // decide whether to merge
      toMergeAndExpand = false;
      if (ms.equals(PrefixMergeStrategy.FULL) && vpa.bytes.length> 1) {
        toMergeAndExpand = true;
      }
      if (ms.equals(PrefixMergeStrategy.PARTIAL)) {
        if (HashMergeEvaluator.evaluateMerge(vpa, preLen, height, keys.length, mt)) {
          partialToMerge.incrementAndGet();
          toMergeAndExpand = true;
        } else {
          partialNotMerge.incrementAndGet();
        }
      }

      // deliver the decision
      if (toMergeAndExpand) {
        // when to execute: Full merge or evaluated worthy, and shared by more than ONE key
        MergePrefix.occ.incrementAndGet();
        MergePrefix.ttlLen.addAndGet(vpa.prd);
        final IMicroNode recNode =
            recNextMergeOnHashV2(
                logicalChild,
                vpa.bytes,
                len + preLen + 1,
                ms,
                mt,
                height);
        // add the node generated in rec to the current node
        repNode.setChild(
            Arrays.copyOfRange(vpa.bytes[0],len + preLen,len+ preLen+ 1),
            recNode);
      } else {
        // go-through to the no-branching child
        for (byte[] k : vpa.bytes) {
          repNode.setChild(Arrays.copyOfRange(k, preLen + len, k.length), logicalChild.apply(k));
        }
      }
    }
    return repNode;
  }

  public static IMicroNode initHashNodeWithPartialKey(
      byte[] key,
      int preLen,
      int len) {
    IMicroNode res = new HNodeVDev();
    res.setParKey(len == 0 ? null : Arrays.copyOfRange(key, preLen, len + preLen));
    return res;
  }
}
