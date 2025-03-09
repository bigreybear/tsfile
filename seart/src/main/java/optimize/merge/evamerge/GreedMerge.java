package optimize.merge.evamerge;

import optimize.MainSupport;
import optimize.MyDataSet;
import optimize.TSTree;
import optimize.merge.MapType;
import optimize.merge.PrefixMergeStrategy;
import optimize.merge.TwoPhasePrefixMerge;
import optimize.merge.skeleton.PartitionInfo;
import optimize.nodes.IMicroNode;
import optimize.nodes.ITSNode;
import optimize.nodes.cdm.ICNode;
import optimize.nodes.logic.LLeaf;
import optimize.nodes.logic.LLeafAnnotated;
import optimize.util.ByteArray;
import org.antlr.v4.runtime.tree.Tree;
import org.openjdk.jol.info.GraphLayout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class GreedMerge {

  private static class DecisionStat {
    static int spaceSaved = 0, toMerge = 0, notMerge = 0;
    int space0, space1;
    DecisionStat(int space0, int space1, boolean merge) {
      this.space0 = space0;
      this.space1 = space1;
      if (merge) {
        spaceSaved += space0 - space1;
        toMerge ++;
      } else {
        notMerge ++;
      }
    }
  }

  private static final float ALPHA = 1f;
  private static final List<DecisionStat> DECISION_STATS = new ArrayList<DecisionStat>();

  static boolean decideToMerge(int s0, int s1, double t0, double t1) {
    boolean res = ALPHA*(s1-s0) + (1-ALPHA)*(t1-t0) < 0;
    DECISION_STATS.add(new DecisionStat(s0, s1, res));
    return res;
  }

  public enum IndexType {
    hash,
    sorted;
  }
  private static final IndexType IT = IndexType.sorted;


  // return the new root
  public static ICNode greedMerge(ITSNode root) {
    ICNode crt;
    MergingArea es = new MergingArea(root);
    es.estimateAndExpand(IT);

    // CNodes only hold CNode child, for a pre-order traverse, while the parent subtree is determined,
    // the descendants are not transformed yet where the recursion occurs.
    TreeMap<ByteArray, ICNode> updatedK2C = new TreeMap<>();
    for (Map.Entry<ByteArray, IMicroNode> entry1 : es.k2c.entrySet()) {
      ICNode res;
      if (!(entry1.getValue().isLogicalLeaf())) {
        res = greedMerge(entry1.getValue());
      } else {
        res = (ICNode) entry1.getValue();
      }
      updatedK2C.put(entry1.getKey(), res);
    }

   // merge on each candidate by recursion
    crt = es.transformToCNode(updatedK2C, IT);
    return crt;
  }


  public static void main(String[] args) {
    MyDataSet ds = MyDataSet.BW;
    TSTree tree = MainSupport.buildLogicalTree(ds, true);
    // transformed into an annotated ART
    TwoPhasePrefixMerge.transformToAnnotatedART(tree);
    traverseAndMarkInfo(tree.root, 0,0 );
    ICNode r = greedMerge(tree.root);
    System.out.println("HERE");

    tree.root = r;
    long lat = MainSupport.estimateLatency(tree, ds, PrefixMergeStrategy.FULL, MapType.CDM);

    long size = GraphLayout.parseInstance(r).totalSize();
    System.out.println(size);
  }

  private static void traverseAndMarkInfo(ITSNode node, int depth, int offset) {
    int parKeyLen = node.getParKeyLen();
    List<ITSNode> children = node.getPhysicalChildren();

    PartitionInfo info = node.getInfoObj();
    info.brPos = offset + parKeyLen;
    info.dep = depth;

    ITSNode chd;
    for (int i = 0, len = children.size(); i < len; i++) {
      if ((chd = children.get(i)).isLogicalLeaf()) { // skip all leaves
        chd.getInfoObj().brPos = Integer.MAX_VALUE;
        continue;
      }

      traverseAndMarkInfo(
          chd,
          depth + 1,
          offset + 1 + parKeyLen);
    }
  }
}
