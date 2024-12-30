package optimize.nodes.cdm;

import static optimize.util.ArrayHelper.removeTrailingZeros;

import optimize.exception.KeyNotFound;
import optimize.nodes.NodeInspector;
import optimize.nodes.NodeWithPartialKey;

public abstract class CNodeBase extends NodeWithPartialKey {
  public byte[][] rmk;
  public ICNode[] ptrs;
  protected static byte[] EMPTY_BYTE_ARR = new byte[0];

  // Note(zx): there are two approaches to init the rmk array:
  //  1. init only if there are interleaved bytes, and orphan leaf would incur
  //  2. init even no interleaved bytes, thus orphans are eliminated
  // an orphan leaf is a CLeaf with no partial key, i.e., a trivial leaf, only representing the dot.
  protected static final boolean NO_ORPHAN_CLEAF = true;

  public ICNode getPtr(int pos) {
    return ptrs[pos];
  }

  protected abstract byte[] getBrKeyAt(int channel);

  int checkKeyBytes(byte[] key, int channel, int[] bps) {
    byte[] brCheck = getBrKeyAt(channel);
    byte[] cmpCheck = rmk == null ? null : rmk[channel];

    // i for brCheck, j for key, r for cmpCheck
    int i = 0, j = bps[0], r = 0;
    while (i < brCheck.length) {
      if (brCheck[i++] != key[j++]) throw new KeyNotFound(key);

      if (cmpCheck == null) continue;

      if (i < brCheck.length) {
        while (j != bps[i]) {
          if (key[j++] != cmpCheck[r++]) throw new KeyNotFound(key);
        }
      } else {
        // the last branching byte has been checked
        while (r < cmpCheck.length) {
          if (key[j++] != cmpCheck[r++]) throw new KeyNotFound(key);
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
}
