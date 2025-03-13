package optimize.merge.evamerge;

import optimize.nodes.IMicroNode;
import optimize.nodes.ITSNode;
import optimize.nodes.cdm.ICNode;
import optimize.nodes.cdm.frame.CNodeBase;
import optimize.nodes.cdm.one.CNodeOneBase;
import optimize.nodes.fdm.IFNode;
import optimize.nodes.logic.LLeaf;
import optimize.nodes.logic.LLeafAnnotated;
import optimize.util.ByteArray;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static optimize.util.ByteArray.concatenate;

public class MergingArea {

  private static final byte[] EMPTY_BYTE_ARR = new byte[0];

  // pos -> children, all outside the merging area
  TreeMap<Integer, Set<IMicroNode>> candidates = new TreeMap<>();

  // positions have been merged
  TreeSet<Integer> brcPos = new TreeSet<>();

  // content inside the merging area
  public Map<ByteArray, IMicroNode> k2c = new HashMap<>();
  Map<ByteArray, byte[]> k2r = new HashMap<>();

  final IFNode miniRoot;

  public MergingArea(ITSNode mrt) {
    // from here on, the par-child relationship is embedded in the MergingArea
    miniRoot = (IFNode) mrt;
    brcPos.add(mrt.getInfoObj().brPos);
    for (byte k : miniRoot.getKeysFromFDM()) {
      IMicroNode c = miniRoot.get(k);
      k2r.put(new ByteArray(k), EMPTY_BYTE_ARR);
      if (c instanceof LLeafAnnotated) {
        k2c.put(new ByteArray(k), new LLeaf((LLeafAnnotated) c));
      } else {
        k2c.put(new ByteArray(k), c);
        updateCandidate(c.getInfoObj().brPos, c);
      }
    }
  }

  public MergingArea(MergingArea ma) {
    miniRoot = ma.miniRoot;
    brcPos.addAll(ma.brcPos);
  }

  public void updateCandidate(int pos, IMicroNode c) {
    candidates.compute(pos, (k, v) -> {
      if (v == null) v = new HashSet<>();
      v.add(c);
      return v;
    });
  }

  public void recordChildren(int pos, Collection<IMicroNode> children) {
    if (!candidates.containsKey(pos)) {
      candidates.put(pos, new HashSet<>());
    }
    candidates.get(pos).addAll(children);
  }

  public ICNode transformToCNode(TreeMap<ByteArray, ICNode> m, EvaMergeConfig.IndexType t) {
    // use the entry value from passing in map

    if (brcPos.size() == 1) {
      return CNodeOneBase.buildNode(miniRoot.getParKey(), m);
    }

    int[] pos =  brcPos.stream().mapToInt(Integer::intValue).toArray();
    return CNodeBase.buildNode(miniRoot.getParKey(), pos, m, k2r, t);
  }

  private MergingArea estimate(EvaMergeConfig.IndexType type) {
    if (candidates.isEmpty()) return null;

    int space0, space1;
    double time0, time1;
    int pm = candidates.firstKey();
    Set<IMicroNode> minPosCandidates = candidates.get(pm);

    // todo calc space: involves all children, but only count par key for those not nearest
    //  time calc should also include all children

    if (brcPos.size() == 1) {
      space0 = EvaHelper.estSpaceNativeART(miniRoot.getParKey(), k2c.size());
      time0 = EvaHelper.estTimeNativeART(k2c.size());
    } else {
      space0 = EvaHelper.estSpaceMultiBranchNode(miniRoot.getParKey(), k2c.size(), k2r.values(), brcPos);
      time0 =
          type == EvaMergeConfig.IndexType.hash
              ? EvaHelper.estTimeMBNHash(k2c.size())
              : EvaHelper.estTimeMBNSorted(k2c.size());
    }

    double ttlChdTime = 0;
    for (IMicroNode c : minPosCandidates) {
      List<IMicroNode> gc = c.getChildren();
      space0 += EvaHelper.estSpaceNativeART(c.getParKey(), gc.size());
      ttlChdTime += EvaHelper.estTimeNativeART(gc.size());
    }
    time0 += ttlChdTime / minPosCandidates.size();

    MergingArea nxtSta = new MergingArea(this);
    boolean updateRMK = pm > brcPos.first() + 1;

    Set<ByteArray> keyB4Exp = k2c.keySet();

    // Note(zx) 要解释为什么计算空间时，brp>pm 的节点不需要考虑：

    int lastBrcPos = brcPos.last();
    Set<IMicroNode> noTrimNodes = new HashSet<>();  // these are children of minPosCandidate, shall not trim
    for (ByteArray kb4 : keyB4Exp) {
      // 为了估计合并后的大小，要为合并后的 CNode 做预计算：
      IFNode cb4 = (IFNode) k2c.get(kb4);
      // prepare to update rmk
      byte[] rmk = k2r.get(kb4);

      if (cb4.isLogicalLeaf()) {
        LLeaf l = cb4 instanceof LLeaf ? (LLeaf) cb4 : new LLeaf((LLeafAnnotated) cb4);
        int span = pm - lastBrcPos;
        int index = span - 1; // position to extract from par key
        ByteArray nk = index >= cb4.getParKeyLen()
            ? new ByteArray(kb4, (byte) 0)
            : new ByteArray(kb4, cb4.getParKey()[index]);
        nxtSta.k2c.put(nk, l);
        nxtSta.k2r.put(nk,
            l.getParKey() == null
                ? rmk
                : concatenate(
                rmk,
                Arrays.copyOfRange(l.getParKey(), 0, Math.min(index, l.getParKeyLen()))));
        continue;
      }

      if (cb4.getInfoObj().brPos < pm) throw new RuntimeException("Exceptional error brPos.");
      if (cb4.getInfoObj().brPos == pm) {
        // incorporate all descent children
        byte[] cb4keys = cb4.getKeysFromFDM();

        for (byte cb4k : cb4keys) {
          ByteArray nk = new ByteArray(kb4, cb4k);
          IMicroNode gcn  = cb4.get(cb4k); // grand child node
          if (gcn instanceof LLeafAnnotated) {
            gcn = new LLeaf((LLeafAnnotated) gcn);
          }
          noTrimNodes.add(gcn);
          nxtSta.k2c.put(nk, gcn);
          // the rmk is inflated, from 1 entry to as many as cb4.children().size()
          if (updateRMK) {
            nxtSta.k2r.put(nk, concatenate(rmk, cb4.getParKey()));
          } else {
            nxtSta.k2r.put(nk, rmk);
          }
        }
      } else {
        // extract one byte from pk to key, and update rmk
        // gap from the working position and this position
        int gap = cb4.getInfoObj().brPos - pm;
        int index = cb4.getParKeyLen() - gap;
        if (index < 0) throw new RuntimeException("Illegal short par key.");
        byte appendByte = cb4.getParKey()[index];
        ByteArray nk = new ByteArray(kb4, appendByte);
        nxtSta.k2c.put(nk, cb4);
        nxtSta.k2r.put(nk,
            updateRMK
                ? concatenate(k2r.get(kb4), Arrays.copyOfRange(cb4.getParKey(), 0, Math.min(index, cb4.getParKeyLen())))
                : k2r.get(kb4));
      }
    }

    nxtSta.brcPos.add(pm);
    space1 = nxtSta.evaSpaceItself();
    time1 = type == EvaMergeConfig.IndexType.hash
        ? EvaHelper.estTimeMBNHash(nxtSta.k2c.size())
        : EvaHelper.estTimeMBNSorted(nxtSta.k2c.size());


    if (GreedMerge.decideToMerge(space0, space1, time0, time1)) {
      // trim all partial keys
      for (IMicroNode c : nxtSta.k2c.values()) {
        if (c instanceof LLeaf) {  // Note(zx) requires no annotated leaf in k2c
          // trim parKey for leaf
          if (c.getParKeyLen() == 0 || noTrimNodes.contains(c)) continue;
          int index = pm-lastBrcPos-1;
          byte[] npk = index + 1 <= c.getParKeyLen()
              ? Arrays.copyOfRange(c.getParKey(), index+1, c.getParKeyLen())
              : null;
          c.setParKey((npk == null || npk.length == 0) ? null : npk);
          continue;
        }

        int gap = c.getInfoObj().brPos - pm;
        int index = c.getParKeyLen() - gap; // index of extracted byte
        if (index >= 0) {
          c.setParKey(Arrays.copyOfRange(c.getParKey(), index + 1, c.getParKeyLen()));
        }

        // update candidates with remaining children
        nxtSta.updateCandidate(c.getInfoObj().brPos, c);
      }
      return nxtSta;
    } else {
      // give up merging, just leave out the expected status
      return null;
    }
  }

  // if it is now transformed into a CNode, estimate the space
  private int evaSpaceItself() {
    if (brcPos.size() == 1) {
      return EvaHelper.estSpaceNativeART(miniRoot.getParKey(), k2c.size());
    } else {
      return EvaHelper.estSpaceMultiBranchNode(miniRoot.getParKey(), k2c.size(), k2r.values(), brcPos);
    }
  }

  public void estimateAndExpand(EvaMergeConfig.IndexType type) {
    MergingArea res = estimate(type), r2;
    while (res != null && res.brcPos.size() < 8 && ((r2 = res.estimate(type)) != null)) {
      res = r2;
    }
    if (res != null) {
      // update this instance
      candidates = res.candidates;
      brcPos = res.brcPos;
      k2c = res.k2c;
      k2r = res.k2r;
    }
  }
}
