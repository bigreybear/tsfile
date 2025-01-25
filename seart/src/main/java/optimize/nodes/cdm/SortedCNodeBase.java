package optimize.nodes.cdm;

import static optimize.merge.CDMPrefixMerge.recMergeCDM;
import static optimize.nodes.cdm.CNodeHelper.extractBytes;
import static optimize.util.ArrayHelper.findComplementary;
import static optimize.util.ArrayHelper.findIntervals;
import static optimize.util.ArrayHelper.removeTrailingZeros;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import optimize.SearchStatus;
import optimize.merge.MapType;
import optimize.merge.PrefixMergeStrategy;
import optimize.nodes.IMicroNode;
import optimize.nodes.NodeInspector;
import optimize.nodes.NodeWithPartialKey;
import optimize.util.InfixGroup;

public non-sealed abstract class SortedCNodeBase extends CNodeBase implements ICNode {

  @Override
  protected final int transformIndex(int idx) {
    return idx;
  }

  protected abstract byte[] getBrKeyAt(int channel); // no trailing 0s.

  /**
   * Checks the key bytes against the branching key and complementary key for a given channel.
   *
   * @param key the key to be checked
   * @param channel the channel index to retrieve the branching key
   * @param bps an array of branching positions
   * @return the index in the key array after the last checked byte
   */
  int checkKeyBytes(byte[] key, int channel, int[] bps) {
    // branching keys are already checked before
    byte[] compCheck = rmk == null ? null : rmk[channel]; // complementary
    if (compCheck == null) return Math.min(bps[bps.length-1] + 1, key.length);

    int bi = 1, ki = bps[0] + 1, ri = 0;
    while (ri != compCheck.length) {
      while (bi < bps.length && bps[bi] == ki + ri) {
        bi++;
        ki++;
      }
      if (compCheck[ri] != key[ri + ki]) throw new RuntimeException("RMK check failed.");
      ri++;
    }
    // if branching keys not exhausted, just set the next to the last branch pos
    return bi < bps.length ? Math.min(bps[bps.length-1] + 1, key.length) : ri+ki;
  }

  public static void main(String[] args) {

  }

  void setInterleavedBytes(int idx, byte[] ilb) {
    if (ilb != null && ilb.length > 0) {
      if (rmk == null) rmk = new byte[ptrs.length][];
      rmk[idx] = removeTrailingZeros(ilb);
    }
  }

  public static int[] byteArr2IntArr(byte[] a) {
    int[] r = new int[a.length];
    for (int i = 0; i < a.length; i++) {
      r[i] = 0xff & a[i];
    }
    return r;
  }

  @Override
  public List<IMicroNode> getChildren() {
    return Arrays.asList(ptrs);
  }

  // region Set Content
  // all methods below shared between CNode2/4/8

  /**
   * Triggered when only one key belongs to the branching key, meaning the pointer could directly
   * point to the logical leaf (or another root if oneTree=false in {@linkplain optimize.Main}).
   *
   * <p>Deeply coupled with {@linkplain #checkKeyBytes}, which check/read the bytes set by this
   * method. A little weird, may be fixed in further days: the channel of idx could hold more
   * positions than others, and this gap is handled by the aforementioned method.
   *
   * @param brPos branching positions
   * @param compKey the complete key
   * @param idx position to set the ptr
   * @param getLChild closure to retrieve the logical child
   */
  private void incorporateTrivialLeaf(
      int[] brPos, byte[] compKey, int idx, Function<byte[], IMicroNode> getLChild) {
    int[] cmpPos = findComplementary(brPos[0], compKey.length - 1, brPos);

    setInterleavedBytes(idx, extractBytes(compKey, cmpPos));
    ptrs[idx] = (ICNode) getLChild.apply(compKey);
  }

  // a supporter function to hide the primitive type discrepancy
  protected abstract Function<Integer, List<byte[]>> generateCompleteKeyRetrieval(InfixGroup group);

  public void setContent(
      InfixGroup group,
      Function<byte[], IMicroNode> getLChild,
      MapType mapType,
      PrefixMergeStrategy mergeStrategy,
      int height) {
    setParKey(group.getCommonPrefix());
    List<byte[]> completeKeys;
    int[] brPos = group.getBranchingPos(), itvPos;
    int sbkSize = group.countBranches();
    // WHY use func if/: varying instance may use varying typed branching keys.
    // i.e. short[]/int[]/long[] cannot be generified so be wrapped by func if/.
    Function<Integer, List<byte[]>> retrieval = generateCompleteKeyRetrieval(group);

    for (int i = 0; i < sbkSize; i++) {
      // do not worry about prefixed key: handled by 0x00 key byte
      completeKeys = retrieval.apply(i);

      // if only one key, needless to recur, set all other bytes as rmk
      if (NO_ORPHAN_CLEAF && completeKeys.size() == 1) {
        incorporateTrivialLeaf(brPos, completeKeys.get(0), i, getLChild);
        continue;
      }

      itvPos = findIntervals(brPos);
      setInterleavedBytes(i, extractBytes(completeKeys.get(0), itvPos));
      ptrs[i] =
          (ICNode)
              recMergeCDM( // previously not V2
                  getLChild,
                  completeKeys,
                  brPos[brPos.length - 1] + 1,
                  mapType,
                  mergeStrategy, height);
    }
  }

  // endregion

  // region Proceed Query CDM

  // supporters for query
  protected abstract int getEmptyKeyIdx();

  // composite the key and search for its index/offset. Note(zx) performance sensitive
  protected abstract int getBrKeyIdx(byte[] key, int[] brPos);

  // body to query
  public ICNode proceedQueryCDM(byte[] key, SearchStatus sts) {
    if (sts.getCurLen() == key.length) {
      int idx = getEmptyKeyIdx();
      sts.setFinished(true);
      return idx < 0 ? this : ptrs[idx];
    }

    int[] bps = getBranchingPos();
    if (bps.length == 0) throw new RuntimeException();
    int curLen = sts.getCurLen();
    curLen = checkPartialKey(key, curLen, bps[0]);

    // finish searching and is PREFIXED
    if (curLen == key.length) {
      sts.setFinished(true);
      return ptrs[getEmptyKeyIdx()];
    }

    int channel = getBrKeyIdx(key, bps);
    sts.setCurLen(checkKeyBytes(key, channel, bps));
    // sts.setFinished(sts.getCurLen() == key.length);
    return ptrs[channel];
  }

  // endregion

  // support for inspect
  abstract protected String codeName();

  @Override
  public void acceptInspector(NodeInspector noi) {
    noi.appendEntry("ptr_num_" + codeName(), ptrs.length);
    int ttl = 0;
    for (int i = 0; rmk != null && i < rmk.length; i++) {
      ttl += rmk[i] == null ? 0 : rmk[i].length;
    }
    if (rmk != null) noi.appendEntry("rmk_num_" + codeName(), rmk.length);
    noi.appendEntry("rmk_ttl_len_" + codeName(), ttl);
    noi.appendEntry("valid_br_pos_len_" + codeName(), getBranchingPos().length);
  }
}
