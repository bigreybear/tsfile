package optimize.merge.bum;

import optimize.merge.skeleton.MiniTreeRep;
import optimize.merge.skeleton.PartitionInfo;
import optimize.nodes.ITSNode;
import optimize.util.ByteArray;

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

/*-
 * Note(zx) README.
 * 这个方法就是对每个 IBNode 进行检查，在 infoObj 做标记，为后续转为 MiniTreeRep 做准备。
 * 这个方法有几个重要决策，当前采取了简单的实现。未来可以更复杂，例如增加理论估算。
 * 主要方法有几个步骤：
 * 1. 收集所有 非 mini-root 孩子的 pos，计算每个 pos 重复的次数；
 * 2. 对每个 mini root 孩子，判断是否有 pos 重叠
 *   2.1 第一个决策点：是仅收集 first few shared positions，还是收集当前节点最后一个 pos 以前的所有 pos
 *   2.2 第二个决策点：是仅从当前 mini-tree 范围内收集，还是包含下述的其他 mini tree
 *   这两个决策点的实现分别是 {@link #getLastSharedPrefixPos} 和 {@link #pushDownMiniRoot}.
 *   选择的都是决策点中较简单的方法。但也有各自的理由。
 *   对于 2.1 选择第一种实现，因为：1）尽可能少地影响这些 mini root 孩子，避免增加高度；2）只有 HCNodeX 才需要
 *   保证 pos 对齐（参见 {@link optimize.merge.skeleton.MiniTreeRep#includeTo}。
 *   对于 2.2 选择第一种实现，因为基于 2.1 的选择，对于任意 mini-root child 与 其自身的孩子即 mini-root grandson 不会再
 *   在 pos 上重合了（此前已遍历检查）。
 * 3. 如上整理出当前节点的预备 pos 集合，如果小于 8 那么可立即返回，如果大于 8 那么要选择部分摘出去
 *   3.1 第一个决策点：是选择将某个孩子摘出（即 seal as mini root），还是指定某个长度，对所有孩子一起 seal
 *   3.2 第二个决策点：如何选择孩子，或者选择高度
 *   此处选择的是某个孩子，3.1 的选择理由与 2.1 类似，3.2 的选择实现见方法中部（根据多个属性排序）。
 *    3.2.1 对于 seal a child node as new mini-root，还要考虑对这个新 mini-root 检查是否有重叠的 pos
 *    这个 child node 本身可能不需要摘出，是其子孙需要 seal，这个检查能把这个孩子再收回来
 *    要注意，如果这个孩子所有 pos 都与当前 mini-tree 重叠，那么就不要立即对其进行 push down，
 *     因为这会导致前面的 seal 失效。
 *    这种情况通常是因为每个 pos 都有多个 child 重叠，只有多次 seal child 后才能知道哪些 pos 是真正应该剔除的。
 *    每次选择 child 都要重新进行排序，因为 pos set 受到上次 seal out 影响了。
 *    3.2.2 若干次 seal out child 后使 pos 数量小于 8，此时再去检查 child mini-root 中重叠的 pos。
 *    此时才获得真正需要的 pos set，而 3.2.1 中没有检查重叠 pos 的节点此时才被正确处理。
 * 4. 更新当前节点 info 并退出。
 *
 * Main function. key idea: <br>
 * 1) when too many positions, seal a node which has the lowest hyper level and many positions; <br>
 * 2) for any mini root child, always pull up common positions. <br>
 *
 *  Pull up position from child mini tree, MEANING push down the mini root.
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

    // choose a child to seal out so that other mini trees keep untouched
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

    // Note(zx) it is necessary as some nodes may not be checked position sharing when sealed out.
    // These nodes have identical pos set as each pos has more than one holder.
    // Only after several nodes are sealed out, the expected pos are finally out of the scope.
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

    info. acc = initAcc;
    info.hyperLevel = initHL;
    info.brPosSet.addAll(weightedPosMap.getPosSet());
  }

  /**
   * For a node is mini root, push down the mini-root seal to its descendants within
   * the original mini-tree. <br>
   * Using a non-recur to traverse ONLY the designated mini tree.
   * @param node from it to push down mini-root seal
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
      // Note(zx) Why not recur on mini-root child:
      // since only checked on shared but not in-range positions, no need to recur mini-roots
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
