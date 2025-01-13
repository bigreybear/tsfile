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
import optimize.exception.KeyNotFound;
import optimize.merge.PrefixMergeStrategy;
import optimize.nodes.IMicroNode;
import optimize.nodes.NodeInspector;
import optimize.nodes.NodeWithPartialKey;
import optimize.util.InfixGroup;

public abstract class CNodeBase extends NodeWithPartialKey implements ICNode {
  public byte[][] rmk; // Re-Mained Keys
  public ICNode[] ptrs;
  protected static byte[] EMPTY_BYTE_ARR = new byte[0];
  protected static int SINGLE_BYTE_MASK = 0xffffff00;

  // Note(zx): there are two approaches to init the rmk array:
  //  1. init only if there are interleaved bytes, and orphan leaf would incur
  //  2. init even no interleaved bytes, thus orphans are eliminated
  // an orphan leaf is a CLeaf with no partial key, i.e., a trivial leaf, only representing the dot.
  protected static final boolean NO_ORPHAN_CLEAF = true;

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
    byte[] brCheck = getBrKeyAt(channel);
    byte[] compCheck = rmk == null ? null : rmk[channel]; // complementary

    // i for brCheck, j for key, r for compCheck
    int i = 0, j = bps[0], r = 0;
    while (i < brCheck.length) {
      if (brCheck[i++] != key[j++]) throw new KeyNotFound(key);

      if (compCheck == null) continue;

      if (i < brCheck.length) {
        while (j != bps[i]) {
          if (key[j++] != compCheck[r++]) throw new KeyNotFound(key);
        }
      } else {
        // the last branching byte has been checked
        while (r < compCheck.length) {
          if (key[j++] != compCheck[r++]) throw new KeyNotFound(key);
        }
      }
    }
    return j;
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

  protected void inspectRMK(NodeInspector ni, String nodePrefix) {
    ni.appendEntry("CNode/4_ptr", ptrs.length);

    if (rmk == null) {
      ni.incEntry(nodePrefix + "_rmk_null", 1);
      return;
    }
    if (rmk.length == 0) {
      ni.incEntry(nodePrefix + "_rmk_empty", 1);
      return;
    }

    int nulNum = 0, ttlLen = 0;
    for (byte[] bytes : rmk) {
      if (bytes == null) nulNum++;
      else ttlLen += bytes.length;
    }
    // ni.incEntry(nodePrefix + "_rmk_null_elem", nulNum);

    // ni.appendEntry(nodePrefix + "_rmk_arr_len", rmk.length);
    // ni.appendEntry(nodePrefix + "_rmk_elem_avg_len", ttlLen);
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

  // a server function, set branching keys at same time
  protected abstract Function<Integer, List<byte[]>> generateCompleteKeyRetrieval(InfixGroup group);

  public void setContent(
      InfixGroup group,
      Function<byte[], IMicroNode> getLChild,
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
                  mergeStrategy,
                  height);
    }
  }

  // endregion

  // region Proceed Query CDM

  // supporters for query
  protected abstract int getEmptyKeyIdx();

  // composite the key and search for its index/offset
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
}
