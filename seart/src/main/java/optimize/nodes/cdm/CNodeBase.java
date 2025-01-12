package optimize.nodes.cdm;

import static optimize.merge.CDMPrefixMerge.recNextMergeOnCDMV2;
import static optimize.nodes.cdm.ByteEncode.int2Bytes;
import static optimize.nodes.cdm.CNodeHelper.extractBytes;
import static optimize.util.ArrayHelper.findComplementary;
import static optimize.util.ArrayHelper.findIntervals;
import static optimize.util.ArrayHelper.removeTrailingZeros;

import optimize.exception.KeyNotFound;
import optimize.merge.PrefixMergeStrategy;
import optimize.nodes.IMicroNode;
import optimize.nodes.NodeInspector;
import optimize.nodes.NodeWithPartialKey;
import optimize.util.InfixGroup;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

public abstract class CNodeBase extends NodeWithPartialKey {
  public byte[][] rmk;  // Re_Mained_Keys
  public ICNode[] ptrs;
  protected static byte[] EMPTY_BYTE_ARR = new byte[0];
  protected static int SINGLE_BYTE_MASK = 0xffffff00;

  // Note(zx): there are two approaches to init the rmk array:
  //  1. init only if there are interleaved bytes, and orphan leaf would incur
  //  2. init even no interleaved bytes, thus orphans are eliminated
  // an orphan leaf is a CLeaf with no partial key, i.e., a trivial leaf, only representing the dot.
  protected static final boolean NO_ORPHAN_CLEAF = true;

  protected abstract byte[] getBrKeyAt(int channel); // no trailing 0s.

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

  public List<IMicroNode> getChildren() {
    return Arrays.asList(ptrs);
  }

  // to solve orphan leaves
  protected final void solveComplementaryLeaf(
      InfixGroup group,
      byte[] compKey,
      int idx,
      Function<byte[], IMicroNode> getLChild) {
    int[] cmpPos =
        findComplementary(
            group.getBranchingPos()[0],
            compKey.length - 1,
            group.getBranchingPos()
        );

    setInterleavedBytes(idx, extractBytes(compKey, cmpPos));
    ptrs[idx] = (ICNode) getLChild.apply(compKey);
  }

  // region Set Content
  // all methods below shared between CNode2/4/8

  // a server function, set branching keys at same time
  abstract protected Function<Integer, List<byte[]>> generateCompleteKeyRetrieval(InfixGroup group);

  public void setContent(
      InfixGroup group,
      Function<byte[], IMicroNode> getLChild,
      PrefixMergeStrategy mergeStrategy,
      int height) {
    List<byte[]> completeKeys;
    int[] itvPos;
    int sbkSize = group.countBranches();
    Function<Integer, List<byte[]>> retrieval = generateCompleteKeyRetrieval(group);

    for (int i = 0; i < sbkSize; i++) {
      // do not worry about prefixed key: handled by 0x00 key byte
      completeKeys = retrieval.apply(i);

      // if only one key, needless to recur, set all other bytes as rmk
      if (NO_ORPHAN_CLEAF && completeKeys.size() == 1) {
        solveComplementaryLeaf(group, completeKeys.get(0), i, getLChild);
        continue;
      }

      itvPos = findIntervals(group.getBranchingPos());
      setInterleavedBytes(i, extractBytes(completeKeys.get(0), itvPos));
      ptrs[i] = (ICNode) recNextMergeOnCDMV2( // previously not V2
          getLChild,
          completeKeys.toArray(new byte[0][0]),
          group.getBranchingPos()[group.getBranchingPos().length - 1] + 1,
          mergeStrategy,
          height
      );
    }
  }

  // endregion
}
