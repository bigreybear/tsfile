package optimize.merge.bum;

import optimize.merge.skeleton.PartitionInfo;
import optimize.nodes.ITSNode;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public abstract class BottomUpMergeStrategy {

  public static final int DIVERGE_FACTOR = 64; // 64 child per branch pos

  // Note(zx) only update the passing in node
  public static void updateNodeInfo(ITSNode n) {
    Deque<ITSNode> nodes = new ArrayDeque<>();
    int acc = 0, hl = 0;
    Set<Integer> posSet = new TreeSet<>();

    nodes.add(n);
    ITSNode c;
    while (!nodes.isEmpty()) {
      c = nodes.removeFirst();

      if (c.isLogicalLeaf()) {
        acc++;
      } else {
        if (c.getInfoObj().isMiniRoot) {
          acc++;
          hl = Math.max(hl, c.getInfoObj().hyperLevel + 1);
        } else {
          posSet.add(c.getInfoObj().brPos);
          nodes.addAll(c.getPhysicalChildren());
        }
      }
    }

    n.getInfoObj().acc = acc;
    n.getInfoObj().hyperLevel = hl;
    n.getInfoObj().brPosSet.clear();
    n.getInfoObj().brPosSet.addAll(posSet);
  }

  public static void recUpdateNodeInfo(ITSNode n) {

    int acc = 0, hl = 0;
    Set<Integer> brSet = new HashSet<>();

    for (ITSNode c : n.getPhysicalChildren()) {
      if (!c.isLogicalLeaf() && !c.getInfoObj().isMiniRoot) {
        recUpdateNodeInfo(c);
        acc += c.getInfoObj().acc;
        hl = Math.max(hl, c.getInfoObj().hyperLevel);
        brSet.addAll(c.getInfoObj().brPosSet);
        continue;
      }

      if(c.isLogicalLeaf()) {
        acc ++;
        continue;
      }

      // c is mini root
      acc++;
      hl = Math.max(hl, c.getInfoObj().hyperLevel + 1);
    }

    n.getInfoObj().acc = acc;
    n.getInfoObj().brPosSet.clear();
    n.getInfoObj().brPosSet.add(n.getInfoObj().brPos);
    n.getInfoObj().brPosSet.addAll(brSet);
    n.getInfoObj().hyperLevel = hl;
  }

  public abstract void mergeAndUpdateInfo(ITSNode node, PartitionInfo info, List<ITSNode> children);

  static Set<Integer> mergeConnectedBrPosSet(List<PartitionInfo> chdInfoLst) {
    Set<Integer> res = new TreeSet<>();
    for (PartitionInfo info : chdInfoLst) {
      if (!info.isMiniRoot) {
        res.addAll(info.brPosSet);
      }
    }
    return res;
  }

  // fixme possibly buggy
  static int maxChdHyperLevel(List<PartitionInfo> chdInfoLst) {
    return chdInfoLst.stream()
        .mapToInt(info -> info.isMiniRoot ? info.hyperLevel + 1 : info.hyperLevel)
        .max()
        .orElse(0);
  }

}
