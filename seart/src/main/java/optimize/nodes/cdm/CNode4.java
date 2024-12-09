package optimize.nodes.cdm;

import static optimize.nodes.cdm.CNodeHelper.bytes2Int;
import static optimize.nodes.cdm.CNodeHelper.extractBytes;
import static optimize.nodes.cdm.CNodeHelper.findIntervals;
import static optimize.nodes.cdm.CNodeHelper.int2BytesFixedLen;
import static optimize.nodes.cdm.CNodeHelper.int2BytesVarLen;
import static optimize.nodes.fdm.vfull.SEARTNode.ubyte;
import static optimize.util.ArrayHelper.removeTrailingZeros;
import static optimize.nodes.cdm.CNodeHelper.setBytesByPosNoCheck;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import optimize.nodes.IInternal;
import optimize.nodes.INode;
import optimize.nodes.IStaticNode;

public class CNode4 implements INode, IInternal, IStaticNode, ICNode {
  // for only 4 positions
  int posInt; // an int concatenated by 4 bytes: byte p1, p2, p3, p4;
  byte[] parKey; // partial keys
  int[] bks; // indeed a byte[][4] bks; // branching keys
  byte[][] rmk; // remaining keys
  public ICNode[] ptrs;

  // exactly no padding on 64-jvm, jdk-17, Compressed OOPs

  // raw keys might with prefix
  public CNode4(int[] pos) {
    if (pos.length > 4)
      throw new UnsupportedOperationException("No more than 4 bytes branching key yet.");

    // pos int init.
    byte[] posBytes = new byte[4];
    for (int i = 0; i < pos.length; i++) {
      if ((pos[i] & 0xffffff00) != 0)
        throw new UnsupportedOperationException("Longer than 255 not supported in CDM yet.");
      posBytes[i] = (byte) (pos[i] & 0x000000ff);
    }
    posInt = CNodeHelper.bytes2Int(posBytes);
  }

  @Override
  public int[] getBranchingPos() {
    return ICNode.unsignedByteArr2IntArr(int2BytesVarLen(posInt));
  }

  @Override
  public void setBranchingKeys(List<Integer> branchingBytes) {
    ptrs = new ICNode[branchingBytes.size()];

    // init interleaved bytes array
    int[] itvPos = findIntervals(int2BytesVarLen(posInt));
    if (itvPos.length > 0) rmk = new byte[branchingBytes.size()][];

    bks = branchingBytes.stream().mapToInt(i -> i).toArray();
  }

  // get index of the target key
  @Override
  public int getBrKeyIdx(int val) {
    int idx = Arrays.binarySearch(bks, val);
    if (idx == -1 || bks[idx] != val) throw new RuntimeException("Key not found.");
    return idx;
  }

  @Override
  public void setBranchingPtr(int idx, INode ptr) {
    ptrs[idx] = (ICNode) ptr;
  }

  @Override
  public void setInterleavedBytes(int idx, byte[] ilb) {
    ilb = removeTrailingZeros(ilb);
    if (ilb.length > 0 && rmk == null)
      throw new RuntimeException("Initial Interleave Bytes Error.");
    if (ilb.length == 0) return;

    rmk[idx] = ilb;
  }

  @Override
  public void setPartialKey(byte[] b) {
    parKey = b;
  }

  @Override
  public byte[] assembleKeyAt(int pos) {
    byte[] res;
    byte[] brKey = int2BytesFixedLen(bks[pos], 4);
    brKey = removeTrailingZeros(brKey);

    int[] brPosInt = ICNode.unsignedByteArr2IntArr(int2BytesVarLen(posInt));
    int[] itvPosInt = findIntervals(brPosInt);

    int keyLen = brPosInt[brPosInt.length-1] - brPosInt[0] + 1;

    int[] brRltPos = ICNode.shiftIntArr(brPosInt, -1 * brPosInt[0]);
    int[] itvRltPos = ICNode.shiftIntArr(itvPosInt, -1 * brPosInt[0]);

    byte[] asmkey = new byte[keyLen];
    // ICNode.setBytesByPos(asmkey, brKey, brRltPos);
    setBytesByPosNoCheck(asmkey, brKey, brRltPos);
    if (rmk != null && rmk[pos] != null) setBytesByPosNoCheck(asmkey, rmk[pos], itvRltPos);
    // ICNode.setBytesByPos(asmkey, rmk[pos], itvRltPos);
    return removeTrailingZeros(asmkey);
  }

  @Override
  public INode replace(String key, INode nNode) {
    return null;
  }

  @Override
  public INode getChild(String name) {
    byte[] kb = name.getBytes(StandardCharsets.UTF_8), cpk, curBrKeys, checkBrKeys;

    ICNode curNode = this;
    int idx = 0; /* idx to read the key */
    int channel = -1; // which ptr to route
    int[] brPos;
    while (idx < kb.length) {
      // check on partial key
      if ((cpk = curNode.getPartialKey()) != null) {
        for (int i = 0; i < cpk.length && idx < kb.length; i++) {
          if (kb[idx] != cpk[i]) throw new RuntimeException("Key not exists: " + name);
          idx++;
        }

        if (idx == kb.length) {
          if (curNode instanceof CLeaf) return ((CLeaf) curNode).ptr;
          // search key is exhausted on partial key, the branching key must be 0000
          channel = curNode.getBrKeyIdx(0);
          curNode = curNode.getPtrByPos(channel);
          break;
        }
      }

      // locate and retrieve brn and itv bytes and verify
      brPos = curNode.getBranchingPos();

      if (brPos == null) {
        // suspect to be a leaf
        break;
      }

      curBrKeys = extractBytes(kb, brPos);
      channel = curNode.getBrKeyIdx(bytes2Int(curBrKeys));
      if (channel < 0) throw new RuntimeException("Key not found: " + name);
      checkBrKeys = curNode.assembleKeyAt(channel);
      for (int i = 0; i < checkBrKeys.length && idx < kb.length; i++) {
        if (checkBrKeys[i] != kb[idx]) throw new RuntimeException();
        idx++;
      }

      curNode = curNode.getPtrByPos(channel);
      if (curNode instanceof CNode) {
        return ((CNode) curNode).getChild(kb, idx);
      }
    }

    // todo fixme IMPROVE
    if (!(curNode instanceof CLeaf)) {
      curNode = curNode.getPtrByPos(curNode.getBrKeyIdx(0));
    }
    return ((CLeaf) curNode).ptr;
  }

  @Override
  public ICNode getPtrByPos(int pos) {
    return ptrs[pos];
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
    return parKey;
  }
}
