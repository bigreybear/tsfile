package optimize.nodes.cdm;

import static optimize.merge.CDMPrefixMerge.recNextMergeOnCDM;
import static optimize.nodes.cdm.CNodeHelper.bytes2Int;
import static optimize.nodes.cdm.CNodeHelper.extractBytes;
import static optimize.nodes.cdm.CNodeHelper.findIntervals;
import static optimize.nodes.cdm.CNodeHelper.int2BytesFixedLen;
import static optimize.nodes.cdm.CNodeHelper.int2BytesVarLen;
import static optimize.nodes.cdm.CNodeHelper.setBytesByPosNoCheck;
import static optimize.util.ArrayHelper.removeTrailingZeros;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import optimize.merge.MapType;
import optimize.merge.PrefixMergeStrategy;
import optimize.nodes.IMicroNode;
import optimize.util.ArrayHelper;
import optimize.util.InfixGroup;

public class CNode4 extends CNodeBase implements ICNode {
  // for only 4 positions
  int posInt; // an int concatenated by 4 bytes: byte p1, p2, p3, p4;
  int[] bks; // indeed a byte[][4] bks; // branching keys

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
  public void setContent(
      InfixGroup group,
      Function<byte[], IMicroNode> getLChild,
      PrefixMergeStrategy mergeStrategy,
      MapType mapType,
      int height,
      boolean EFCoded) {
    List<byte[]> completeKeys;
    int[] itvPos = findIntervals(group.getBranchingPos());
    int[] sortedBrKeys = group.sortedBrKeys();
    setBranchingKeys(sortedBrKeys);

    // curNode.setContent(Arrays.stream(sortedBrKeys).boxed().collect(Collectors.toList()));
    int validBrKeyLen = group.getBranchingPos().length;
    for (int i = 0; i < sortedBrKeys.length; i++) {
      // do not worry about prefixed key: handled by 0x00 key byte
      completeKeys = group.getCompleteKeys(int2BytesFixedLen(sortedBrKeys[i], validBrKeyLen));

      setInterleavedBytes(i, extractBytes(completeKeys.get(0), itvPos));
      setBranchingPtr(
          i,
          (ICNode) recNextMergeOnCDM(
              getLChild,
              completeKeys.toArray(new byte[0][0]),
              group.getBranchingPos()[group.getBranchingPos().length - 1] + 1,
              mergeStrategy,
              mapType,
              height,
              EFCoded));
    }
  }

  public void setBranchingKeys(int[] collected) {
    bks = new int[collected.length];
    ptrs = new ICNode[collected.length];
    System.arraycopy(collected, 0, bks, 0, bks.length);
    // init interleaved bytes array
    int[] itvPos = findIntervals(int2BytesVarLen(posInt));
    if (itvPos.length > 0) rmk = new byte[collected.length][];
  }

  public void setBranchingKeys(List<Integer> branchingBytes) {
    ptrs = new ICNode[branchingBytes.size()];

    // init interleaved bytes array
    int[] itvPos = findIntervals(int2BytesVarLen(posInt));
    if (itvPos.length > 0) rmk = new byte[branchingBytes.size()][];

    bks = branchingBytes.stream().mapToInt(i -> i).toArray();
  }

  // get index of the target key
  public int getBrKeyIdx(int val) {
    int idx = Arrays.binarySearch(bks, val);
    if (idx == -1 || bks[idx] != val) throw new RuntimeException("Key not found.");
    return idx;
  }

  private void setBranchingPtr(int idx, ICNode ptr) {
    ptrs[idx] = ptr;
  }

  private void setInterleavedBytes(int idx, byte[] ilb) {
    ilb = removeTrailingZeros(ilb);
    if (ilb.length > 0 && rmk == null)
      throw new RuntimeException("Initial Interleave Bytes Error.");
    if (ilb.length == 0) return;

    rmk[idx] = ilb;
  }


  @Override
  public byte[] assembleKeyAt(int pos) {
    byte[] res;
    byte[] brKey = int2BytesFixedLen(bks[pos], 4);
    brKey = removeTrailingZeros(brKey);

    int[] brPosInt = ICNode.unsignedByteArr2IntArr(int2BytesVarLen(posInt));
    int[] itvPosInt = findIntervals(brPosInt);

    int keyLen = brPosInt[brPosInt.length - 1] - brPosInt[0] + 1;

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
  public IMicroNode getChild(byte[] k) {
    if (k.length > 4) throw new UnsupportedOperationException();
    int ans = bytes2Int(k);
    int idx = getBrKeyIdx(ans);
    return ptrs[idx];
  }

  @Override
  public void setChild(byte[] k, IMicroNode n) {
    ptrs[getBrKeyIdx(k)] = (ICNode) n;
  }

  public byte[][] getKeysFromCDM() {
    byte[][] res = new byte[bks.length][];
    for (int i = 0; i < bks.length; i++) {
      res[i] = int2BytesFixedLen(bks[i], 4);
    }
    return res;
  }

  @Override
  public void replace(byte[] key, IMicroNode nNode) {
    setChild(key, nNode);
  }


  public int getBrKeyIdx(byte[] ba) {
    return getBrKeyIdx(bytes2Int(ba));
  }

  /**
   * Adapted from public INode getChild(String name) {
   */
  @Override
  public IMicroNode getLogicalChild(String name) {
    byte[] kb = name.getBytes(StandardCharsets.UTF_8), cpk, curBrKeys, checkBrKeys;

    ICNode curNode = this;
    int idx = 0; /* idx to read the key */
    int channel = -1; // which ptr to route
    int[] brPos;
    while (idx < kb.length) {
      // check on partial key
      if ((cpk = curNode.getParKey()) != null) {
        for (int i = 0; i < cpk.length && idx < kb.length; i++) {
          if (kb[idx] != cpk[i]) throw new RuntimeException("Key not exists: " + name);
          idx++;
        }

        if (idx == kb.length) {
          if (curNode instanceof CLeaf) return ((CLeaf) curNode).ptr;
          // search key is exhausted on partial key, the branching key must be 0000
          channel = curNode.getBrKeyIdx(0);
          curNode = curNode.getPtr(channel);
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

      curNode = curNode.getPtr(channel);
      if (curNode instanceof CNode) {
        return ((CNode) curNode).getChild(kb, idx);
      }
    }

    // todo fixme IMPROVE
    if (!(curNode instanceof CLeaf)) {
      curNode = curNode.getPtr(curNode.getBrKeyIdx(0));
    }
    return ((CLeaf) curNode).ptr;
  }

  @Override
  public long getValue() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<byte[]> getKeyBytes() {
    return Arrays.stream(bks).mapToObj(CNodeHelper::int2BytesVarLen).collect(Collectors.toList());
  }

  @Override
  public List<IMicroNode> getChildren() {
    return Arrays.asList(ptrs);
  }

}
