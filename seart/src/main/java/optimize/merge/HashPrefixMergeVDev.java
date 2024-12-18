package optimize.merge;

import static optimize.merge.MergePrefix.partialNotMerge;
import static optimize.merge.MergePrefix.partialToMerge;
import static optimize.nodes.cdm.CNodeHelper.findLCPLength;
import static optimize.nodes.cdm.CNodeHelper.groupPrefixes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import optimize.nodes.IMicroNode;
import optimize.nodes.cdm.CNodeHelper;
import optimize.nodes.hash.HNodeVDev;

public class HashPrefixMergeVDev {

  /**
   * An experimental function, may be less read-friendly comparing to the V1 function.
   *
   * @param logicalChild wrapping oriNode for its getChild function
   */
  public static IMicroNode recNextMergeOnHashV2VDev(
      Function<byte[], IMicroNode> logicalChild,
      byte[][] keys,
      int preLen,
      PrefixMergeStrategy ms,
      MapType mt,
      int height) {

    final int len = findLCPLength(keys, preLen);
    final IMicroNode repNode = initHashNodeWithPartialKey(keys[0], preLen, len);

    // nothing shared, just transform the nodes
    if (len == 0 && ms.equals(PrefixMergeStrategy.SIMPLE)) {
      for (byte[] key : keys) {
        repNode.setChild(key, logicalChild.apply(key));
      }
      return repNode;
    }

    List<byte[]> longerKeys = new ArrayList<>();
    for (byte[] k : keys) {
      if (k.length == len + preLen) {
        if (repNode.getChild(k) != null) throw new RuntimeException("Duplicate Keys.");
        repNode.setChild(new byte[0], logicalChild.apply(k));
      } else if (k.length > len + preLen) {
        longerKeys.add(k);
      } else {
        throw new RuntimeException("Shall be no shorter keys.");
      }
    }

    if (ms.equals(PrefixMergeStrategy.SIMPLE)) {
      // all keys longer than prefix will be added AS IS
      for (byte[] nk : longerKeys) {
        repNode.setChild(Arrays.copyOfRange(nk, preLen + len, nk.length), logicalChild.apply(nk));
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
      if (ms.equals(PrefixMergeStrategy.FULL) && vpa.bytes.length > 1) {
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
            recNextMergeOnHashV2VDev(logicalChild, vpa.bytes, len + preLen + 1, ms, mt, height);
        // add the node generated in rec to the current node
        repNode.setChild(Arrays.copyOfRange(vpa.bytes[0], len + preLen, len + preLen + 1), recNode);
      } else {
        // go-through to the no-branching child
        for (byte[] k : vpa.bytes) {
          repNode.setChild(Arrays.copyOfRange(k, preLen + len, k.length), logicalChild.apply(k));
        }
      }
    }
    return repNode;
  }

  public static IMicroNode initHashNodeWithPartialKey(byte[] key, int preLen, int len) {
    IMicroNode res = new HNodeVDev();
    res.setParKey(len == 0 ? null : Arrays.copyOfRange(key, preLen, len + preLen));
    return res;
  }
}
