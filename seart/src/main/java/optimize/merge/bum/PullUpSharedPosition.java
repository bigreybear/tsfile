package optimize.merge.bum;

import optimize.merge.skeleton.PartitionInfo;
import optimize.nodes.ITSNode;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static optimize.merge.bum.SealMark.chosenToSeal;
import static optimize.merge.bum.SealMark.exact8;
import static optimize.merge.bum.SealMark.pushDown;

/**
 * key idea: <br>
 * 1) when too many positions, seal a node which has the lowest hyper level and many positions; <br>
 * 2) for any mini root child, always pull up common positions.
 *
 */
public class PullUpSharedPosition extends BottomUpMergeStrategy{
  @Override
  public void mergeAndUpdateInfo(ITSNode on, PartitionInfo info, List<ITSNode> children) {
    // get branch positions from non-root children
    // pull up common prefix from existed mini-root
    // choose and seal target non-root child as mini-root
    // check acc makes it best for 2/4/8 positions

    // count position and its weight
    WeightedPositions weightedPosMap = new WeightedPositions();
    for (ITSNode c : children) {
      if (!c.isLogicalLeaf() && !c.getInfoObj().isMiniRoot) {
        weightedPosMap.addAll(c.getInfoObj().brPosSet);
      }
    }

    // pull up positions from mini-roots
    for (ITSNode c : children) {
      if (!c.isLogicalLeaf() && c.getInfoObj().isMiniRoot) {
        int commonPos = getLastSharedPrefixPos(c.getInfoObj().brPosSet, weightedPosMap);
        // decide if it should push down
        if (commonPos >= 0) {
          pushDownMiniRoot(c, commonPos);
          weightedPosMap.addAll(c.getInfoObj().brPosSet);
        }
      }
    }

    // quick exit
    if (weightedPosMap.size() <= 7) {
      // Note(zx) leave it not sealed with exact8 makes it more flexible for further merge
      // if (weightedPosMap.size() == 7) info.sealMiniRoot(true, exact8);

      // updateNodeInfo(on); todo is it enough to update only one?
      recUpdateNodeInfo(on);
      info.brPosSet.addAll(weightedPosMap.getPosSet());
      return;
    }

    while (weightedPosMap.size() > 7) {
      // init the list
      List<NodeSealValue> valuedChildLst = new ArrayList<>();
      for (ITSNode c : children) {
        if (c.isLogicalLeaf() || c.getInfoObj().isMiniRoot) continue;
        int un = 0, overlapFactor = 0;
        for (Integer p : c.getInfoObj().brPosSet) {
          // calculate the number of unique position provided by the node
          if (weightedPosMap.get(p) == 1) un++;
          overlapFactor += weightedPosMap.get(p);
        }
        valuedChildLst.add(new NodeSealValue(c, un, (double) overlapFactor / c.getInfoObj().brPosSet.size()));
      }

      // Note(zx) key to the strategy: choose which to seal/exclude?
      // sort by hyper level to try to avoid increment tree height;
      // sort by uniPosNum to reduce brPosNum as quick as possible;
      // sort by acc to make new mini-tree more fruitful
      valuedChildLst.sort(Comparator
          .comparingInt((NodeSealValue e) -> e.node.getInfoObj().hyperLevel)
          .thenComparingInt(e -> -e.uniPosNum)
          .thenComparingDouble(e -> e.avgOverlap)
          .thenComparingInt(e -> -e.node.getInfoObj().acc));

      int firstPushIdx = 0;
      ITSNode sealNode;
      do {
        sealNode = valuedChildLst.get(firstPushIdx).node;
        if (!sealNode.isLogicalLeaf() && !sealNode.getInfoObj().isMiniRoot) break;
        firstPushIdx++;
      } while (true);


      weightedPosMap.subAll(sealNode.getInfoObj().brPosSet);
      int lastPos = ((TreeSet<Integer>)sealNode.getInfoObj().brPosSet).last();
      int lastShared = getLastSharedPrefixPos(sealNode.getInfoObj().brPosSet, weightedPosMap);

      // first try to reduce brPosNum by sealing the node,
      // then add back the shared positions
      sealNode.getInfoObj().sealMiniRoot(true, chosenToSeal);
      // if lastPos == lastShared, the pushDown will just revert the seal
      if (lastShared >= 0 && lastPos != lastShared) {
        pushDownMiniRoot(sealNode, lastShared);
        weightedPosMap.addAll(sealNode.getInfoObj().brPosSet);
      }
    }

    // Note(zx) fixme todo INTERESTING, duplicate with previous one
    // This block will make sure current node will NOT overlaps with direct-child-mini-tree
    // however it may increase tree height.
    // will it undermine performance?
    for (ITSNode c : children) {
      if (!c.isLogicalLeaf() && c.getInfoObj().isMiniRoot) {
        int commonPos = getLastSharedPrefixPos(c.getInfoObj().brPosSet, weightedPosMap);
        // decide if it should push down
        if (commonPos >= 0) {
          pushDownMiniRoot(c, commonPos);
          weightedPosMap.addAll(c.getInfoObj().brPosSet);
        }
      }
    }

    int initAcc = 0, initHL = 0;
    for (ITSNode c : children) {
      initAcc += (c.isLogicalLeaf() || c.getInfoObj().isMiniRoot) ? 1 : c.getInfoObj().acc;
      initHL = (!c.isLogicalLeaf() && c.getInfoObj().isMiniRoot)
          ? Math.max(c.getInfoObj().hyperLevel + 1, initHL)
          : Math.max(initHL, c.getInfoObj().hyperLevel);
    }

    info.acc = initAcc;
    info.hyperLevel = initHL;
    info.brPosSet.addAll(weightedPosMap.getPosSet());
  }

  /**
   * Notice this only updates additional info.
   * @param tar the deepest to be pulled up
   */
  public static void pushDownMiniRoot(ITSNode node, int tar) {
    // set descendents with larger brPos than tar as mini-root
    // update info of each node
    node.getInfoObj().isMiniRoot = false;
    Deque<ITSNode> nodes = new ArrayDeque<>();
    nodes.add(node);
    ITSNode cur;
    while (!nodes.isEmpty()) {
      cur = nodes.removeFirst();
      if (cur.isLogicalLeaf() || cur.getInfoObj().isMiniRoot) continue;
      if (cur.getInfoObj().brPos > tar) {
        // need no update: its descendants are untouched
        cur.getInfoObj().sealMiniRoot(true, pushDown);
        cur.getInfoObj().sealReason = String.format("push down %d from %d ", tar, node.getInfoObj().brPos);
        continue;
      }
      nodes.addAll(cur.getPhysicalChildren());
    }
    // updateNodeInfo(node);
    recUpdateNodeInfo(node);
  }

  static int getLastSharedPrefixPos(Set<Integer> cur, WeightedPositions map) {
    List<Integer> curPos = new ArrayList<>(cur);
    curPos.sort(Comparator.comparingInt(i-> i));
    int i = -1;
    for (Integer p : curPos) {
      if (!map.contains(p)) {
        break;
      }
      i = p;
    }
    return i;
  }

  private static class WeightedPositions {
    private final Map<Integer, Integer> map = new TreeMap<>();

    boolean contains(int k) {return map.containsKey(k);}

    int get(int k) {return map.getOrDefault(k, 0);}

    void add(int pos) {
      map.compute(pos, (k, v) -> v == null ? 1 : v + 1);
    }

    void addAll(Collection<Integer> col) {
      for (Integer i : col) add(i);
    }

    void sub(int pos) {
      map.compute(pos, (k, v) -> v == 1 ? null : v - 1);
    }

    void subAll(Collection<Integer> col) {
      for (Integer i : col) sub(i);
    }

    int size() {return map.size();}

    Set<Integer> getPosSet() {return map.keySet();}
  }

  // record the value to seal a node as mini root
  private static class NodeSealValue {
    public ITSNode node;
    public int uniPosNum;
    public double avgOverlap;

    NodeSealValue(ITSNode c, int upn, double olf) {node = c; uniPosNum = upn; avgOverlap = olf;}
  }
}
