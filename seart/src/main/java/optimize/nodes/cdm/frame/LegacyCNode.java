package optimize.nodes.cdm.frame;

import static optimize.nodes.cdm.CNodeHelper.extractBytes;
import static optimize.nodes.cdm.CNodeHelper.getValidBrPosNum;
import static optimize.util.ArrayHelper.findIntervals;
import static optimize.util.ArrayHelper.removeTrailingZeros;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import optimize.SearchStatus;
import optimize.merge.MapType;
import optimize.merge.PrefixMergeStrategy;
import optimize.nodes.IMicroNode;
import optimize.nodes.NodeInspector;
import optimize.nodes.cdm.CNodeHelper;
import optimize.nodes.cdm.ICNode;
import optimize.nodes.logic.LLeaf;
import optimize.util.ArrayHelper;
import optimize.util.ByteArray;
import optimize.util.InfixGroup;

@Deprecated
public non-sealed class LegacyCNode extends CNodeBase implements ICNode {
  // for more than 4 positions
  byte[][] bks;
  byte[] pos; // indeed flags for byte p1, p2, p3, p4;

  public LegacyCNode(int[] pi) {
    pos = new byte[pi.length];
    for (int i = 0; i < pi.length; i++) {
      if (pi[i] > 255) throw new UnsupportedOperationException("Too big branching pos.");
      pos[i] = (byte) (0xff & pi[i]);
    }
  }

  public static ICNode buildCDMTemplate(ICNode node) {
    byte[][] keys = node.getBranchingKeys();
    int[] fakePos = new int[keys[0].length];

    // why to sort: CNode4 is sorted by int and could be different from byte[]
    Arrays.sort(keys, Arrays::compare);
    LLeaf leaf;
    LegacyCNode tr = new LegacyCNode(fakePos);

    // todo remove member access to instance method
    tr.bks = keys;
    for (int i = 0; i < keys.length; i++) {
      leaf = new LLeaf(i);
      leaf.setParKey(node.getChild(keys[i]).getParKey());
      tr.ptrs[i] = leaf;
    }
    return tr;
  }

  @Override
  public byte[][] getBranchingKeys() {
    return bks;
  }

  @Override
  public ICNode getChild(byte[] k) {
    return ptrs[getBrKeyIdx(removeTrailingZeros(k))];
  }

  @Deprecated
  public IMicroNode getChild(byte[] name, int preLen) {
    int ki = preLen;
    byte[] partialKey = getParKey();
    if (getParKey() != null) {
      for (int i = 0; i < partialKey.length; i++) {
        if (name[ki] != partialKey[i]) {
          throw new RuntimeException("Key not consistent with partial key");
        }
        ki++;
      }
    }

    // int idx = getBrKeyIdx(removeTrailingZeros(Arrays.copyOfRange(name, ki, name.length)));
    int idx =
        getBrKeyIdx(
            ArrayHelper.removeTrailingZeros(
                extractBytes(name, ICNode.unsignedByteArr2IntArr(pos))));
    byte[] checkKey = assembleKeyAt(idx, preLen, name.length);
    for (int i = 0; i < checkKey.length; i++) {
      if (name[ki + i] != checkKey[i])
        throw new UnsupportedOperationException("Inconsistent on assemble key.");
    }
    return ptrs[idx];
  }

  @Override
  public void setContent(
      InfixGroup group,
      Function<byte[], IMicroNode> getLChild,
      MapType mapType, PrefixMergeStrategy mergeStrategy,
      int height) {
    byte[][] input = group.sortedBrKeyBytes();
    bks = new byte[input.length][];
    for (int i = 0; i < input.length; i++) {
      bks[i] = ArrayHelper.removeTrailingZeros(input[i]);
    }
    // rmk = new byte[input.length][];
    ptrs = new ICNode[input.length];

    byte[] sk, ck;
    List<byte[]> ckl;
    for (int i = 0; i < input.length; i++) {
      sk = input[i];
      ckl = group.getInfixMap().get(new ByteArray(sk));
      if (ckl.size() > 1) {
        throw new UnsupportedOperationException(
            "Too long key: " + new String(ckl.get(0), StandardCharsets.UTF_8));
      }
      ck = ckl.get(0);

      int[] cmpPos =
          ArrayHelper.findComplementary(
              group.getBranchingPos()[0], ck.length, group.getBranchingPos());
      setInterleavedBytes(i, extractBytes(ck, cmpPos));
      ptrs[i] = (ICNode) getLChild.apply(ck);
    }
  }

  @Override
  public ICNode proceedQueryCDM(final byte[] key, SearchStatus sts) {
    if (sts.getCurLen() == key.length) {
      int idx = getBrKeyIdx(EMPTY_BYTE_ARR);
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
      return ptrs[getBrKeyIdx(EMPTY_BYTE_ARR)];
    }

    // retrieve related br_keys and cmp_keys
    int channel = getBrKeyIdx(removeTrailingZeros(extractBytes(key, bps)));

    sts.setCurLen(checkKeyBytes(key, channel, bps));
    // sts.setFinished(sts.getCurLen() == key.length);
    return ptrs[channel];
  }

  @Override
  protected String codeName() {
    return null;
  }

  @Override
  public int[] getBranchingPos() {
    int[] pi = new int[pos.length];
    for (int i = 0; i < pos.length; i++) {
      pi[i] = 0xff & pos[i];
    }
    return pi;
  }

  private int getBrKeyIdx(byte[] ba) {
    int left = 0, right = bks.length - 1;

    while (left <= right) {
      int mid = left + (right - left) / 2;

      int cmp = Arrays.compare(bks[mid], ba);

      if (cmp == 0) {
        return mid;
      } else if (cmp < 0) {
        left = mid + 1;
      } else {
        right = mid - 1;
      }
    }
    return -1;
  }

  @Override
  public byte[] assembleKeyAt(int tarPos) {
    int[] posInt = ICNode.unsignedByteArr2IntArr(pos);
    int[] itvInt = findIntervals(posInt);

    int[] brRltPos = ICNode.shiftIntArr(posInt, -1 * posInt[0]);
    int[] itvRltPos = ICNode.shiftIntArr(itvInt, -1 * itvInt[0]);

    byte[] asmkey = new byte[posInt[posInt.length - 1] - posInt[0] + 1];
    CNodeHelper.setBytesByPosNoCheck(asmkey, bks[tarPos], brRltPos);

    if (rmk != null && rmk.length != 0)
      CNodeHelper.setBytesByPosNoCheck(asmkey, rmk[tarPos], itvRltPos);
    return removeTrailingZeros(asmkey);
  }

  public byte[] assembleKeyAt(int tarPos, int preLen, int keyLen) {
    preLen = pk == null ? preLen : preLen + pk.length;

    int[] posInt = ICNode.unsignedByteArr2IntArr(pos);
    int[] itvInt = ArrayHelper.findComplementary(preLen, keyLen, posInt);

    int[] brRltPos = ICNode.shiftIntArr(posInt, -1 * preLen);
    int[] itvRltPos = ICNode.shiftIntArr(itvInt, -1 * preLen);

    byte[] asmkey = new byte[getValidBrPosNum(keyLen, posInt) + itvInt.length];
    CNodeHelper.setBytesByPosNoCheck(asmkey, bks[tarPos], brRltPos);

    if (rmk != null && rmk.length != 0)
      CNodeHelper.setBytesByPosNoCheck(asmkey, rmk[tarPos], itvRltPos);
    return removeTrailingZeros(asmkey);
  }

  /** Adapted from public IMicroNode getLogicalChild(byte[] name, int preLen) { */
  @Override
  public IMicroNode getLogicalChild(String pathSeg) {
    int preLen = 0;
    byte[] name = pathSeg.getBytes(StandardCharsets.UTF_8);
    int ki = preLen;
    if (pk != null) {
      for (int i = 0; i < pk.length; i++) {
        if (name[ki] != pk[i]) {
          throw new RuntimeException("Key not consistent with partial key");
        }
        ki++;
      }
    }

    // int idx = getBrKeyIdx(removeTrailingZeros(Arrays.copyOfRange(name, ki, name.length)));
    int idx =
        getBrKeyIdx(
            ArrayHelper.removeTrailingZeros(
                extractBytes(name, ICNode.unsignedByteArr2IntArr(pos))));
    byte[] checkKey = assembleKeyAt(idx, preLen, name.length);
    for (int i = 0; i < checkKey.length; i++) {
      if (name[ki + i] != checkKey[i])
        throw new UnsupportedOperationException("Inconsistent on assemble key.");
    }
    return ptrs[idx];
  }

  public List<String> getKeys() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<byte[]> getKeyBytes() {
    return Arrays.asList(bks);
  }

  @Override
  protected void setBranchKeyValRmk(Map<ByteArray, ICNode> m, Map<ByteArray, byte[]> k2r) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected byte[] getBrKeyAt(int channel) {
    return bks[channel];
  }

  @Override
  protected int getEmptyKeyIdx() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected int getBrKeyIdx(byte[] key, int[] brPos) {
    throw new UnsupportedOperationException();
  }

  public static void main(String[] args) {}

  @Override
  protected Object[] setBrKeysAndGenSupFunctions(InfixGroup group) {
    throw new UnsupportedOperationException();
  }

}
