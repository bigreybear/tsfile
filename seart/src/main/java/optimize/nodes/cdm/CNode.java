package optimize.nodes.cdm;

import static optimize.nodes.cdm.CNodeHelper.extractBytes;
import static optimize.nodes.cdm.CNodeHelper.findIntervals;
import static optimize.nodes.cdm.CNodeHelper.getValidBrPosNum;
import static optimize.util.ArrayHelper.removeTrailingZeros;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import optimize.merge.MapType;
import optimize.merge.PrefixMergeStrategy;
import optimize.nodes.IMicroNode;
import optimize.nodes.INode;
import optimize.util.ArrayHelper;
import optimize.util.ByteArray;
import optimize.util.InfixGroup;

public class CNode extends CNodeBase implements ICNode {
  // for more than 4 positions
  byte[] pos; // indeed flags for byte p1, p2, p3, p4;
  byte[][] bks; // branching keys
  byte[][] rmk; // remaining keys
  ICNode[] ptrs;

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
  public ICNode<byte[]> getChildByBytes(byte[] k) {
    byte[] k2 = removeTrailingZeros(k);
    int idx = getBrKeyIdx(k2);
    return ptrs[getBrKeyIdx(k2)];
  }

  @Override
  public void setContent(InfixGroup group, Function<byte[], IMicroNode> getLChild, PrefixMergeStrategy mergeStrategy, MapType mapType, int height, boolean EFCoded) {
    byte[][] input = group.sortedBrKeyBytes();
    bks = new byte[input.length][];
    for (int i = 0; i < input.length; i++) {
      bks[i] = ArrayHelper.removeTrailingZeros(input[i]);
    }
    rmk = new byte[input.length][];
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
          CNodeHelper.complementaryBytePos(
              group.getBranchingPos()[0], ck.length, group.getBranchingPos());
      setInterleavedBytes(i, extractBytes(ck, cmpPos));
      ptrs[i] = (ICNode) getLChild.apply(ck);
    }
  }

  // @Override
  // public void setBranchingKeys(byte[][] input) {
  //   bks = new byte[input.length][];
  //   for (int i = 0; i < input.length; i++) {
  //     bks[i] = ArrayHelper.removeTrailingZeros(input[i]);
  //   }
  //   rmk = new byte[input.length][];
  //   ptrs = new ICNode[input.length];
  //   ptrs[0].setBranchingKeys(new Object[3]);
  // }

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
    preLen = pk == null ? preLen : preLen + pk.length;

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

  public INode getChild(byte[] name, int preLen) {
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

  @Override
  public List<INode> getChildren() {
    return Arrays.asList(ptrs);
  }

  @Override
  public List<String> getKeys() {
    return null;
  }

  @Override
  public INode addChild(String name, INode child) {
    throw new UnsupportedOperationException();
  }

  public static void main(String[] args) {}
}
