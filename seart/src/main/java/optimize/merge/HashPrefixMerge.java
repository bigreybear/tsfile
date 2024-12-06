package optimize.merge;

import static optimize.nodes.cdm.CNodeHelper.findLCPLength;
import static optimize.nodes.cdm.CNodeHelper.groupPrefixes;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import optimize.Evaluator;
import optimize.MergePrefix;
import optimize.nodes.INode;
import optimize.nodes.cdm.CNodeHelper;
import optimize.nodes.hash.HNode;
import optimize.nodes.hash.PrefixedHNode;

public class HashPrefixMerge {
  /**
   * PRIMARY; enter with preLen=0
   *
   * @param oriNode the logical node, containing the original key-node mapping
   * @param preLen entry for 0
   * @param height the logical distance from the whole root
   */
  public static INode recNextMergeOnHash(
      INode oriNode,
      byte[][] keys,
      int preLen,
      Evaluator.MergeStrategy ms,
      Evaluator.MapType mt,
      int height) {

    final int len = findLCPLength(keys, preLen);

    if (len == 0) {
      // simple only extract direct common prefix which is none here
      if (ms.equals(Evaluator.MergeStrategy.SIMPLE)) return oriNode;
      // where partial and all strategy diff from simple
    }

    // find the key exactly IS the common prefix
    INode preNode = null;
    if (preLen + len != 0) {
      List<byte[]> a =
          Arrays.stream(keys).filter(e -> e.length == len + preLen).collect(Collectors.toList());
      if (!a.isEmpty()) {
        preNode = oriNode.getChild(new String(a.get(0), StandardCharsets.UTF_8));
      }
    }

    // the node is now merging
    // todo how thread knows the key if there is a partial merge, for hash?
    //  the ans is, query twice,
    //  first for the remaining, then for first byte
    //  Anyway to improve?
    // the node must replace the original one
    final INode repNode = initNodeWithPartialKey(keys[0], preLen, len, mt, preNode, 0);

    if (ms.equals(Evaluator.MergeStrategy.SIMPLE)) {
      // all keys longer than prefix will be added AS IS
      List<byte[]> longerKeys =
          Arrays.stream(keys).filter(e -> e.length > len + preLen).collect(Collectors.toList());
      for (byte[] nk : longerKeys) {
        repNode.addChild(
            new String(
                Arrays.copyOfRange(nk, preLen + len, nk.length), StandardCharsets.ISO_8859_1),
            oriNode.getChild(new String(nk, StandardCharsets.UTF_8)));
      }
      MergePrefix.occ.incrementAndGet();
      MergePrefix.ttlLen.addAndGet(longerKeys.size() * len);
      return repNode;
    }

    // for partial or all, recursively group keys after prefix and merge again
    List<CNodeHelper.ValuedPrefixArray> groupedPrefix = groupPrefixes(keys, len + preLen, 1);
    boolean eva;
    for (CNodeHelper.ValuedPrefixArray vpa : groupedPrefix) {
      if (((eva = Evaluator.evaluateMerge(vpa, preLen, height, keys.length, mt))
              || ms.equals(Evaluator.MergeStrategy.FULL))
          && vpa.bytes.length > 1) {
        // when to execute: Full merge or evaluated worthy, and shared by more than ONE key

        MergePrefix.occ.incrementAndGet();
        MergePrefix.ttlLen.addAndGet(vpa.prd);
        final INode recNode =
            recNextMergeOnHash(oriNode, vpa.bytes, len + preLen + 1, ms, mt, height);
        // add the node generated in rec to the current node
        repNode.addChild(
            new String(
                // index by first byte after common current prefix (len+preLen)
                Arrays.copyOfRange(
                    vpa.bytes[0],
                    len + preLen,
                    len
                        + preLen
                        + 1 /* previously: vpa.len, but would dup with the pk on descendant*/),
                StandardCharsets.ISO_8859_1),
            recNode);
      } else {
        // go-through to the no-branching child
        for (byte[] k : vpa.bytes) {
          repNode.addChild(
              new String(
                  Arrays.copyOfRange(k, preLen + len, k.length), StandardCharsets.ISO_8859_1),
              oriNode.getChild(new String(k, StandardCharsets.UTF_8)));
        }
      }
    }
    return repNode;
  }

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

    if (len == 0) {
      // simple only extract direct common prefix which is none here
      if (ms.equals(Evaluator.MergeStrategy.SIMPLE)) return logicalChild.apply(null);
      // where partial and all strategy diff from simple

      // todo remove debug
      // System.out.println("DIFF");
    }

    // find the key exactly IS the common prefix
    INode preNode = null;
    if (preLen + len != 0) {
      List<byte[]> a =
          Arrays.stream(keys).filter(e -> e.length == len + preLen).collect(Collectors.toList());
      if (!a.isEmpty()) {
        preNode =
            logicalChild.apply(
                a.get(0)); // oriNode.getChild(new String(a.get(0), StandardCharsets.UTF_8));
      }
    }

    // the node is now merging
    // todo how thread knows the key if there is a partial merge, for hash?
    //  the ans is, query twice,
    //  first for the remaining, then for first byte
    //  Anyway to improve?
    // the node must replace the original one
    final INode repNode = initNodeWithPartialKey(keys[0], preLen, len, mt, preNode, 0);

    if (ms.equals(Evaluator.MergeStrategy.SIMPLE)) {
      // all keys longer than prefix will be added AS IS
      List<byte[]> longerKeys =
          Arrays.stream(keys).filter(e -> e.length > len + preLen).collect(Collectors.toList());
      for (byte[] nk : longerKeys) {
        repNode.addChild(
            new String(
                Arrays.copyOfRange(nk, preLen + len, nk.length), StandardCharsets.ISO_8859_1),
            logicalChild.apply(nk));
        // oriNode.getChild(new String(nk, StandardCharsets.UTF_8)));
      }
      MergePrefix.occ.incrementAndGet();
      MergePrefix.ttlLen.addAndGet(longerKeys.size() * len);
      return repNode;
    }

    // for partial or all, recursively group keys after prefix and merge again
    List<CNodeHelper.ValuedPrefixArray> groupedPrefix = groupPrefixes(keys, len + preLen, 1);
    boolean eva;
    for (CNodeHelper.ValuedPrefixArray vpa : groupedPrefix) {
      if (((eva = Evaluator.evaluateMerge(vpa, preLen, height, keys.length, mt))
              || ms.equals(Evaluator.MergeStrategy.FULL))
          && vpa.bytes.length > 1) {
        // when to execute: Full merge or evaluated worthy, and shared by more than ONE key

        MergePrefix.occ.incrementAndGet();
        MergePrefix.ttlLen.addAndGet(vpa.prd);
        final INode recNode =
            recNextMergeOnHash(
                logicalChild.apply(null), /* oriNode, */
                vpa.bytes,
                len + preLen + 1,
                ms,
                mt,
                height);
        // add the node generated in rec to the current node
        repNode.addChild(
            new String(
                // index by first byte after common current prefix (len+preLen)
                Arrays.copyOfRange(
                    vpa.bytes[0],
                    len + preLen,
                    len
                        + preLen
                        + 1 /* previously: vpa.len, but would dup with the pk on descendant*/),
                StandardCharsets.ISO_8859_1),
            recNode);
      } else {
        // go-through to the no-branching child
        for (byte[] k : vpa.bytes) {
          repNode.addChild(
              new String(
                  Arrays.copyOfRange(k, preLen + len, k.length), StandardCharsets.ISO_8859_1),
              logicalChild.apply(k)
              // oriNode.getChild(new String(k, StandardCharsets.UTF_8))
              );
        }
      }
    }
    return repNode;
  }

  public static INode initNodeWithPartialKey(
      byte[] key,
      int preLen,
      int len,
      Evaluator.MapType mapType,
      INode prefixedChild,
      int branchingNum) {
    switch (mapType) {
      case HASH:
        {
          if (prefixedChild == null) {
            HNode res = new HNode();
            res.pk = len == 0 ? null : Arrays.copyOfRange(key, preLen, len + preLen);
            return res;
          }
          PrefixedHNode res = new PrefixedHNode();
          res.pk = Arrays.copyOfRange(key, preLen, len + preLen);
          res.prePtr = prefixedChild;
          return res;
        }
      case FDM:
        {
          return null;
        }
    }

    return null;
  }
}
