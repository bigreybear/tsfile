package optimize.merge.evamerge;

import optimize.nodes.IMicroNode;
import optimize.nodes.ITSNode;
import optimize.nodes.cdm.ICNode;
import optimize.nodes.cdm.frame.CNodeBase;
import optimize.nodes.cdm.one.CNodeOneBase;
import optimize.nodes.fdm.IFNode;
import optimize.util.ByteArray;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static optimize.merge.evamerge.EvaMergeConfig.ALPHA;
import static optimize.merge.evamerge.EvaMergeConfig.INDEX_TYPE;
import static optimize.merge.evamerge.EvaMergeConfig.LOAD_FACTOR;
import static optimize.util.ArrayHelper.removeTrailingZeros;

/**
 * 给定一个 BNode，计算合并 1-8 层的 时空效率、平均节点的时空效率，最后决定是否转换
 * 在决策转换前，所有节点都维持不变，仅改变 BNode 中的 info 信息（主要是 pk 的起点）
 *
 * 对 Area 的状态更新十分清晰了
 *
 * 决策要考虑合并 1-8 层，再用独立的方法决策究竟采取哪种；
 * 设计一个方法，从原节点直接到合并 n 层的情况
 * 即使如此都不要对原树做变化，直到最后再 transform
 */
public class MergingStatusV2 {

  private static final byte[] EMPTY_BYTE_ARR = new byte[0];

  // init by constructor
  final IFNode miniRoot;
  final int validParKeyIdx;
  TreeSet<Integer> brcPos = new TreeSet<>();
  Set<IMicroNode> coveredNodes = new HashSet<>(); // in the area

  /**
   * Following members are constructed during merge procession.<p>
   * candidates = near + wait, children of covered ones <br>
   * Branch Fan Out: the fan out number it incurs in original ART path <br>
   * val: [chd:IMicNode, rmk:byte[], validParKeyStart:int, ancestors:IMicroNode[]]
   */
  Map<ByteArray, Object[]> candidKeysMaps = new HashMap<>();  // mainly used to track brk and rmk
  // these two are quick index for candidKeyMaps
  TreeMap<Integer, Set<IMicroNode>> candidInternal = new TreeMap<>();
  Set<IMicroNode> candidLeaf = new HashSet<>();

  PerfScore perfScore; /* [merged_numb:int, cad_num:int, space:int, read_exp:double] */

  private void putCandidateKeyMap(ByteArray k, Object... vals) {
    candidKeysMaps.put(k, vals);
  }

  private MergingStatusV2(ITSNode s, int vpki) {
    miniRoot = (IFNode) s;
    validParKeyIdx = vpki;
    coveredNodes.add(miniRoot);
    brcPos.add(s.getInfoObj().brPos);
    for (byte k : miniRoot.getKeysFromFDM()) {
      IMicroNode c = miniRoot.get(k);
      updateCandidateCollections(c, new ByteArray(k), EMPTY_BYTE_ARR, 0, new IMicroNode[] {miniRoot});
    }
  }

  private MergingStatusV2(MergingStatusV2 pre) {
    miniRoot = pre.miniRoot;
    validParKeyIdx = pre.validParKeyIdx;
    brcPos.addAll(pre.brcPos);
    coveredNodes.addAll(pre.coveredNodes);
  }

  // region Merge Procession

  // return the new status after the merge, DO NOT estimate performance
  private MergingStatusV2 proceedMerge() {
    if (candidInternal.isEmpty()) throw new RuntimeException("Shall be ended by outer loop");

    MergingStatusV2 res = new MergingStatusV2(this);
    int bl = brcPos.last(), bm = candidInternal.firstKey();

    Set<ByteArray> curKeys = candidKeysMaps.keySet();
    for (ByteArray bk : curKeys) {
      Object[] e = candidKeysMaps.get(bk);
      IFNode cad = (IFNode) e[0];
      byte[] rmk = (byte[]) e[1];
      int parKeyBeg = (int) e[2];
      IMicroNode[] ancestors = (IMicroNode[]) e[3];
      int baw = cad.getInfoObj().parBranchPos;

      if (!cad.isLogicalLeaf() && cad.getInfoObj().brPos == bm) {
        IMicroNode[] ua = appendElement(ancestors, cad);
        // expand one key to many
        // prolong rmk if needed
        if (bm > bl + 1) {
          int pb = bl - baw;
          assert pb == parKeyBeg;
          assert cad.getParKeyLen() >= pb; // only leaf can have larger pb
          int rmkLen = rmk == null ? 0 : rmk.length;
          byte[] nrmk = new byte[rmkLen + cad.getParKeyLen() - pb];
          if (rmkLen > 0) {
            System.arraycopy(rmk, 0, nrmk, 0, rmkLen);
          }
          if (cad.getParKeyLen() > pb) {
            System.arraycopy(cad.getParKey(), pb, nrmk, rmkLen, cad.getParKeyLen()-pb);
          }
          rmk = nrmk;
        }

        for (byte ek : cad.getKeysFromFDM()) {
          ByteArray nbk = new ByteArray(bk, ek);
          res.updateCandidateCollections(cad.get(ek), nbk, rmk, 0, ua);
        }
        res.coveredNodes.add(cad);
        continue;
      }

      // Note(zx) as the child is not merged, it is still a candidate, a leaf is ALWAYS a candidate

      // only truncate from par key, calc target indices
      final int pks = bl - baw;      // Par Key copying Start, since part of pk may have been added to rmk
      final int pke = bm - baw - 2;  // Par Key copying End (INCLUDED)
      final int bktIdx = bm - baw - 1;  // Branch Key Tailing Index in par key, see pks <= bktIdx
      final byte[] pk = cad.getParKey();

      ByteArray nbk;
      if (pk == null || pk.length <= bktIdx) {
        if (!cad.isLogicalLeaf()) throw new RuntimeException("Internal-Wait candidate should have enough pk.");
        nbk = new ByteArray(bk, (byte) 0);
      } else {
        nbk = new ByteArray(bk, pk[bktIdx]);
      }

      // bm == bl+1: no need to update rmk
      if (bm >= bl + 2) { //  ==> bktIdx > pks
        // update rmk
        int rmkLen = rmk == null ? 0 : rmk.length;
        byte[] nrmk = new byte[rmkLen + bm - bl - 1];
        if (rmkLen != 0) System.arraycopy(rmk, 0, nrmk, 0, rmkLen);

        if (pk == null || pk.length <= pks) { // all 0s
          // Arrays.fill(nrmk, rmkLen, nrmk.length, (byte) 0);
          // nrmk = rmk;  // debug: shall pad nothing
          nrmk = EMPTY_BYTE_ARR;  // Note(zx) TRICK NOTICE: null rmk will skip check
        } else if (pk.length <= bktIdx - 1) { // partial padding
          System.arraycopy(pk, pks, nrmk, rmkLen, pk.length-pks);
          nrmk = Arrays.copyOfRange(nrmk, 0, rmkLen + pk.length - pks); // debug
          // Arrays.fill(nrmk, rmkLen + pk.length - pks, nrmk.length, (byte) 0);
        } else { // no padding
          System.arraycopy(pk, pks, nrmk, rmkLen, bktIdx - pks);
        }

        rmk = nrmk;
      }

      res.updateCandidateCollections(cad, nbk, rmk, bktIdx+1, ancestors);
    }

    res.brcPos.add(bm);
    return res;
  }

  /** Couples with {@link #candidKeysMaps} */
  private void updateCandidateCollections(
      IMicroNode candid, ByteArray bkr,
      byte[] rmk, int vpks, IMicroNode[] anc /*valid partal key start*/) {
    putCandidateKeyMap(bkr, candid, rmk, vpks, anc);
    if (candid.isLogicalLeaf()) {
      candidLeaf.add(candid);
    } else {
      updateCandidInternal(candid.getInfoObj().brPos, candid);
    }
  }

  private void updateCandidInternal(int pos, IMicroNode c) {
    candidInternal.compute(pos, (k, v) -> {
      if (v == null) v = new HashSet<>();
      v.add(c);
      return v;
    });
  }

  private boolean shouldMerge() {
    // todo: also false if perf score too low
    if (candidInternal.isEmpty() || brcPos.size() == 8) {
      return false;
    }

    return true;
  }

  // endregion

  // entry to merge
  public static MergingStatusV2 optimizeMergeArea(ITSNode start, int vpki){
    TreeMap<Integer, MergingStatusV2> records = new TreeMap<>();
    MergingStatusV2 sta = new MergingStatusV2(start, vpki);
    PerfScore pps;

    sta.estimatePerf(null);
    records.put(sta.brcPos.size(), sta);
    while (sta.shouldMerge()) {
      pps = sta.perfScore;
      sta = sta.proceedMerge();
      sta.estimatePerf(pps);
      records.put(sta.brcPos.size(), sta);
    }

    sta = sortByScore(records);
    return sta;
  }

  private void estimatePerf(PerfScore pvs) {
    perfScore = new PerfScore();
    perfScore.evaluate(pvs);
  }

  private int evaluateCoverAreaSpc() {
    return brcPos.size() == 1
        ? EvaHelper.estSpcNativeARTV2(miniRoot)
        : EvaHelper.estSpaceMultiBranchNodeV2(miniRoot.getParKey(), candidKeysMaps, brcPos, (float) LOAD_FACTOR);
  }

  private static MergingStatusV2 sortByScore(Map<Integer, MergingStatusV2> recs) {
    if (recs == null || recs.isEmpty()) {
      return null;
    }

    MergingStatusV2 result = null;
    double minScore = Double.MAX_VALUE;

    for (MergingStatusV2 status : recs.values()) {
      if (status != null && status.perfScore != null) {
        double score = status.perfScore.finalScore;
        if (score < minScore) {
          minScore = score;
          result = status;
        }
      }
    }

    return result;
  }

  public ICNode transformToCDM() {
    // if (!candidInternal.isEmpty()) throw new RuntimeException("Shall solve its internal candidates first");

    int initialCapacity = Math.max(16, candidKeysMaps.size());
    float loadFactor = 0.75f;

    Map<ByteArray, ICNode> m2k = new HashMap<>(initialCapacity, loadFactor);
    Map<ByteArray, byte[]> m2r = new HashMap<>(initialCapacity, loadFactor);

    for (Map.Entry<ByteArray, Object[]> entry : candidKeysMaps.entrySet()) {
      ByteArray key = entry.getKey();
      Object[] values = entry.getValue();

      if (values != null && values.length >= 2) {
        m2k.put(key, (ICNode) values[0]);
        m2r.put(key, (byte[]) values[1]);
      } else {
        throw new RuntimeException();
      }
    }

    byte[] vpk = miniRoot.getParKey();
    if (vpk != null && vpk.length != 0) {
      vpk = Arrays.copyOfRange(vpk, validParKeyIdx, vpk.length);
    }

    if (brcPos.size() == 1) {
      return CNodeOneBase.buildNode(vpk, m2k);
    }

    int[] pos =  brcPos.stream().mapToInt(Integer::intValue).toArray();
    return CNodeBase.buildNode(vpk, pos, m2k, m2r, INDEX_TYPE);
  }

  public static <T> T[] appendElement(T[] original, T element) {
    // gen by Claude
    if (original == null) {
      throw new NullPointerException("Original array cannot be null");
    }
    Class<?> componentType = original.getClass().getComponentType();
    int length = original.length;
    @SuppressWarnings("unchecked")
    T[] newArray = (T[]) Array.newInstance(componentType, length + 1);
    System.arraycopy(original, 0, newArray, 0, length);
    newArray[length] = element;
    return newArray;
  }

  // using ratio, rather than absolute value to make decision
  private class PerfScore {
    // coverNodNum = prev.nearNodeNum + prev.coverNodNum
    int covExpSpc; // covered area expected space
    int covNodNum;
    int covRawSpc; // raw space: node space before merging, prev.covRawSpc + prev.nearSpc

    int nearSpc; // near area is what next to be merged, raw space
    int nearNodNum;

    double spcEff;  // compression ratio of covered space:  covExpSpc / covRawSpc
    PerfScore prev;

    double cadRawLat; // latency per candidate before merge
    double cadExpLat;
    double latEff;  // ratio of merged latency (computed by candidate number) to previous one:

    // alpha * spcEff + (1-alpha) * latEff, lower the better
    double finalScore; // computed by alpha

    private MergingStatusV2 host() {return MergingStatusV2.this; }

    private void evaluate(PerfScore pvs) {
      if (!candidInternal.isEmpty()) {
        int bm = candidInternal.firstKey();
        Set<IMicroNode> nearCads = candidInternal.get(bm);
        nearNodNum = nearCads.size();
        nearSpc = 0;
        for (IMicroNode ci : nearCads) {
          nearSpc += EvaHelper.estSpcNativeARTV2(ci);
        }
      } else {
        nearNodNum = 0;
        nearSpc = 0;
      }

      covNodNum = coveredNodes.size();
      // Note(zx) not exactly accurate: some partial key bytes from other candidates should be excluded
      covExpSpc = evaluateCoverAreaSpc();
      if (pvs != null) {
        covRawSpc = pvs.covRawSpc + pvs.nearSpc;
        spcEff = 1.0d * (covExpSpc) / (covRawSpc);
        prev = pvs;
      } else {
        covRawSpc = covExpSpc;
        spcEff = 1.0d;
        prev = null;
      }

      // candidate exp lat: measure from cadKeyMap number
      if (EvaMergeConfig.INDEX_TYPE == EvaMergeConfig.IndexType.sorted) {
        cadExpLat = EvaHelper.estTimeMBNSorted(candidKeysMaps.size());
      } else {
        cadExpLat = EvaHelper.linerProbeHashLatency(LOAD_FACTOR, candidKeysMaps.size());
      }

      // candidate raw lat: for each candidate, measure latency from its ancestor path;
      // get average from all candis
      double ttlRawlat = 0d;
      for (Map.Entry<?, Object[]> entry : candidKeysMaps.entrySet()) {
        IMicroNode[] ancs = (IMicroNode[]) entry.getValue()[3];
        ttlRawlat += EvaMergeConfig.NODE_OBJ_ACC * (ancs.length - 1);
        for (int i = 0; i < ancs.length; i++) {
          ttlRawlat += EvaHelper.estTimeART((IFNode) ancs[i]);
        }
      }
      cadRawLat = ttlRawlat / candidKeysMaps.size();
      latEff = cadExpLat / cadRawLat;

      finalScore = EvaMergeConfig.ALPHA * spcEff + (1-ALPHA) * latEff;
    }
  }
}
