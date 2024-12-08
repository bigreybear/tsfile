package optimize;

import static optimize.Evaluator.MapType;
import static optimize.Evaluator.MergeStrategy;
import static optimize.Main.CDM_WITH_EF;
import static optimize.Main.REPORT_CHANNEL;
import static optimize.merge.CDMPrefixMerge.recNextMergeOnCDM;
import static optimize.merge.HashPrefixMerge.recNextMergeOnHashV2;
import static optimize.nodes.cdm.CNodeHelper.strings2ByteArrays;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import optimize.merge.CDMPrefixMerge;
import optimize.merge.FDMPrefixMerge;
import optimize.nodes.INode;
import optimize.nodes.hash.HNodeV2;
import optimize.nodes.logic.LLeaf;
import org.openjdk.jol.info.GraphLayout;

public class MergePrefix {
  public static AtomicInteger occ = new AtomicInteger(),
      ttlLen = new AtomicInteger(),
      inc = new AtomicInteger(),
      partialToMerge = new AtomicInteger(),
      partialNotMerge = new AtomicInteger();

  public static void reportMergeStatus(MergeStrategy ms) {
    REPORT_CHANNEL.append(
        String.format("Merge occ: %d, total len: %d, inc: %d \n", occ.get(), ttlLen.get(), inc.get()));
    REPORT_CHANNEL.append(String.format("Partial merge: %d, not merge: %d \n",
        partialToMerge.get(), partialNotMerge.get()));
  }

  /**
   * @param par the parent logical node
   * @return a function which returns node with corresponding key byte[], or parent itself with null
   */
  public static Function<byte[], INode> getLogicalChild(final INode par) {
    return (b -> b == null ? par : par.getChild(new String(b, StandardCharsets.UTF_8)));
  }

  // public static void testRecNextMerge(String[] args) {
  public static void main(String[] args) {
    HNodeV2 n1 = new HNodeV2();
    byte[][] keys =
        new byte[][] {
          "aaabcg".getBytes(StandardCharsets.UTF_8),
          "aaabc".getBytes(StandardCharsets.UTF_8),
          "aaabcgxxab".getBytes(StandardCharsets.UTF_8),
          "aaabcgxxdb".getBytes(StandardCharsets.UTF_8),
          "edf".getBytes(StandardCharsets.UTF_8),
        };

    for (byte[] k : keys) {
      n1.add(k, new LLeaf(k.length));
    }

    // INode res = recNextMergeOnHash(n1, keys, 0, MergeStrategy.PARTIAL, MapType.HASH, 1);
    INode res = FDMPrefixMerge.recNextMergeOnFDM(n1, keys, 0, MergeStrategy.FULL, MapType.FDM, 1);
    System.out.println(GraphLayout.parseInstance(res).totalSize());
    reportMergeStatus(MergeStrategy.PARTIAL);
    INode a = res.getChild("aaabcgxxab");
    System.out.println("HELLO");
  }

  public static final List<String> dupPaths = new ArrayList<>();

  public static void mergePrefixes(TSTree tree, MapType mt, MergeStrategy ms) {
    switch (mt) {
      case CDM:
        tree.traversePostOrderRec(
            (par, key, cur, stk) -> {
              List<String> keyList = null;
              if ((keyList = cur.getKeys()) == null) {
                return;
              }
              byte[][] keyBytes = strings2ByteArrays(keyList);

              INode n2 =
                  recNextMergeOnCDM(
                      getLogicalChild(cur), keyBytes, 0, ms, mt, stk.size(), CDM_WITH_EF);

              if (n2 != cur) {
                if (par == null) {
                  tree.root = n2;
                } else {
                  par.replace(key, n2);
                }
              }
            });
        CDMPrefixMerge.reportMergeStatus();
        return;
      case FDM:
        tree.traversePostOrderRec(
            (par, key, cur, stk) -> {
              List<String> keyList = null;
              if ((keyList = cur.getKeys()) == null) {
                return;
              }
              byte[][] keyBytes = strings2ByteArrays(keyList);

              INode n2 = FDMPrefixMerge.recNextMergeOnFDM(cur, keyBytes, 0, ms, mt, stk.size());

              if (n2 != cur) {
                if (par == null) {
                  tree.root = n2;
                } else {
                  par.replace(key, n2);
                }
              }
            });
        reportMergeStatus(ms);
        return;
      case HASH:
        tree.traversePostOrderRec(
            (par, key, cur, stk) -> {
              List<String> keyList = null;
              if ((keyList = cur.getKeys()) == null) {
                return;
              }
              byte[][] keyBytes = strings2ByteArrays(keyList);

              INode n2 = recNextMergeOnHashV2(getLogicalChild(cur), keyBytes, 0, ms, mt,
              stk.size());  // v2;

              if (n2 != cur) {
                if (par == null) {
                  tree.root = n2;
                } else {
                  par.replace(key, n2);
                }
              }
            });
        reportMergeStatus(ms);
    }
  }
}
