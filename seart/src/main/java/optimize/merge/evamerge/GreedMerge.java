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

import static optimize.MainSupport.dottedNanoSec;

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

  static boolean decideToMerge(int s0, int s1, double t0, double t1) {
    boolean res = ALPHA*(s1-s0) + (1-ALPHA)*(t1-t0) < 0;
    DECISION_STATS.add(new DecisionStat(s0, s1, res));
    return res;
  }

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


  public enum IndexType {
    hash,
    sorted;
  }
  private static final IndexType IT = IndexType.sorted;
  private static final float ALPHA = 0f;
  private static final List<DecisionStat> DECISION_STATS = new ArrayList<DecisionStat>();

  public static void mainInternal(MyDataSet dataSet, boolean estSpace) {
    System.out.println("DataSet: " + dataSet.name());
    MyDataSet ds = dataSet;
    TSTree tree = MainSupport.buildLogicalTree(ds, true);
    // transformed into an annotated ART
    TwoPhasePrefixMerge.transformToAnnotatedART(tree);
    traverseAndMarkInfo(tree.root, 0,0 );
    ICNode r = greedMerge(tree.root);
    // System.out.println("HERE");

    tree.root = r;
    long lat = MainSupport.estimateLatency(tree, ds, PrefixMergeStrategy.FULL, MapType.CDM);

    if (estSpace) {
      long size = GraphLayout.parseInstance(r).totalSize();
      System.out.println(size);
    }
    System.out.println(dottedNanoSec(lat));
  }

  // todo list:
  //  1. include all children into space/time estimation;
  //  2. benchmark (gen code by LLM) search estimations;
  //  3. improve space-saving estimation: consider saved-space per node
  //  4. perfect hash at cost of a few parameter
  public static void main(String[] args) {
    mainInternal(MyDataSet.BW, true);
    mainInternal(MyDataSet.BW, false);
    mainInternal(MyDataSet.BW, false);

    mainInternal(MyDataSet.SW, true);
    mainInternal(MyDataSet.SW, false);
    mainInternal(MyDataSet.SW, false);

    mainInternal(MyDataSet.XYZC, true);
    mainInternal(MyDataSet.XYZC, false);
    mainInternal(MyDataSet.XYZC, false);

    mainInternal(MyDataSet.ZY, true);
    mainInternal(MyDataSet.ZY, false);
    mainInternal(MyDataSet.ZY, false);
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
