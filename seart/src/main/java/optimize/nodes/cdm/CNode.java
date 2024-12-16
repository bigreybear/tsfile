package optimize.nodes.cdm;

import static optimize.nodes.cdm.CNodeHelper.extractBytes;
import static optimize.nodes.cdm.CNodeHelper.findIntervals;
import static optimize.nodes.cdm.CNodeHelper.getValidBrPosNum;
import static optimize.util.ArrayHelper.removeTrailingZeros;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import optimize.nodes.IInternal;
import optimize.nodes.INode;
import optimize.util.ArrayHelper;

public class CNode implements ICNode, INode, IInternal {
  // for more than 4 positions
  byte[] pos; // indeed flags for byte p1, p2, p3, p4;
  byte[] partialKeys; // partial keys
  byte[][] bks; // branching keys
  byte[][] rmk; // remaining keys
  public INode[] ptrs;

  public CNode(int[] pi) {
    pos = new byte[pi.length];
    for (int i = 0; i < pi.length; i++) {
      if (pi[i] > 255) throw new UnsupportedOperationException("Too big branching pos.");
      pos[i] = (byte) (0xff & pi[i]);
    }
  }

  @Override
  public byte[][] getKeysFromCDM() {
    return bks;
  }

  @Override
  public INode getChildByBytes(byte[] k) {
    byte[] k2 = removeTrailingZeros(k);
    int idx = getBrKeyIdx(k2);
    return ptrs[getBrKeyIdx(k2)];
  }

  @Override
  public void setBranchingKeys(List<Integer> collect) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setBranchingKeysExtended(byte[][] input) {
    bks = new byte[input.length][];
    for (int i = 0; i < input.length; i++) {
      bks[i] = ArrayHelper.removeTrailingZeros(input[i]);
    }
    rmk = new byte[input.length][];
    ptrs = new INode[input.length];
  }

  @Override
  public int[] getBranchingPos() {
    int[] pi = new int[pos.length];
    for (int i = 0; i < pos.length; i++) {
      pi[i] = 0xff & pos[i];
    }
    return pi;
  }

  @Override
  public int getBrKeyIdx(int val) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getBrKeyIdx(byte[] ba) {
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
  public void setBranchingPtr(int idx, INode ptr) {
    ptrs[idx] = ptr;
  }

  @Override
  public void setInterleavedBytes(int idx, byte[] ilb) {
    if (ilb == null) {
      rmk[idx] = null;
    }
    ilb = removeTrailingZeros(ilb);
    rmk[idx] = ilb.length == 0 ? null : ilb;
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
    preLen = partialKeys == null ? preLen : preLen + partialKeys.length;

    int[] posInt = ICNode.unsignedByteArr2IntArr(pos);
    int[] itvInt = CNodeHelper.complementaryBytePos(preLen, keyLen, posInt);

    int[] brRltPos = ICNode.shiftIntArr(posInt, -1 * preLen);
    int[] itvRltPos = ICNode.shiftIntArr(itvInt, -1 * preLen);

    byte[] asmkey = new byte[getValidBrPosNum(keyLen, posInt) + itvInt.length];
    CNodeHelper.setBytesByPosNoCheck(asmkey, bks[tarPos], brRltPos);

    if (rmk != null && rmk.length != 0)
      CNodeHelper.setBytesByPosNoCheck(asmkey, rmk[tarPos], itvRltPos);
    return removeTrailingZeros(asmkey);
  }

  @Override
  public ICNode getPtrByPos(int pos) {
    return (ICNode) ptrs[pos];
  }

  @Override
  public void setPartialKey(byte[] b) {
    partialKeys = b;
  }

  @Override
  public INode replace(String key, INode nNode) {
    return null;
  }

  @Override
  public INode replace(byte[] key, INode nNode) {
    return ptrs[getBrKeyIdx(key)] = nNode;
  }

  @Override
  public INode getChild(String name) {
    return getChild(name.getBytes(StandardCharsets.UTF_8), 0);
  }

  public INode getChild(byte[] name, int preLen) {
    int ki = preLen;
    if (partialKeys != null) {
      for (int i = 0; i < partialKeys.length; i++) {
        if (name[ki] != partialKeys[i]) {
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
  public List<INode> getChildren() {
    return Arrays.asList(ptrs);
  }

  @Override
  public List<String> getKeys() {
    return null;
  }

  @Override
  public byte[] getPartialKey() {
    return partialKeys;
  }

  @Override
  public INode addChild(String name, INode child) {
    throw new UnsupportedOperationException();
  }

  public static void main(String[] args) {}
}
