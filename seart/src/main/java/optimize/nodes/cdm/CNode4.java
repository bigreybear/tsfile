package optimize.nodes.cdm;

import static optimize.merge.CDMPrefixMerge.recNextMergeOnCDM;
import static optimize.nodes.cdm.ByteEncode.bytes2Int;
import static optimize.util.ArrayHelper.findComplementary;
import static optimize.nodes.cdm.CNodeHelper.extractBytes;
import static optimize.util.ArrayHelper.findIntervals;
import static optimize.nodes.cdm.ByteEncode.int2Bytes;
import static optimize.nodes.cdm.ByteEncode.int2BytesNoTrailing;
import static optimize.nodes.cdm.CNodeHelper.setBytesByPosNoCheck;
import static optimize.util.ArrayHelper.removeTrailingZeros;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import optimize.SearchStatus;
import optimize.merge.PrefixMergeStrategy;
import optimize.nodes.IMicroNode;
import optimize.nodes.NodeInspector;
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
    posInt = ByteEncode.bytes2Int(posBytes);
  }

  @Override
  public int[] getBranchingPos() {
    // todo improve perf. for query process
    return ICNode.unsignedByteArr2IntArr(
        removeTrailingZeros(
            int2Bytes(posInt)
        )
    );
  }

  @Override
  public void setContent(
      InfixGroup group,
      Function<byte[], IMicroNode> getLChild,
      PrefixMergeStrategy mergeStrategy,
      int height) {
    List<byte[]> completeKeys;
    int[] itvPos;
    int[] sortedBrKeys = group.sortedIntBranchKeys();

    setBranchingKeys(sortedBrKeys);

    for (int i = 0; i < sortedBrKeys.length; i++) {
      // do not worry about prefixed key: handled by 0x00 key byte
      completeKeys = group.getCompleteKeys(int2Bytes(sortedBrKeys[i]));

      // if only one key, needless to recur
      if (NO_ORPHAN_CLEAF && completeKeys.size() == 1) {
        int[] cmpPos =
            findComplementary(
                group.getBranchingPos()[0],
                completeKeys.get(0).length - 1,
                group.getBranchingPos()
            );

        setInterleavedBytes(i, extractBytes(completeKeys.get(0), cmpPos));
        ptrs[i] = (ICNode) getLChild.apply(completeKeys.get(0));
        continue;
      }

      itvPos = findIntervals(group.getBranchingPos());
      setInterleavedBytes(i, extractBytes(completeKeys.get(0), itvPos));
      setBranchingPtr(
          i,
          (ICNode)
              recNextMergeOnCDM(
                  getLChild,
                  completeKeys.toArray(new byte[0][0]),
                  group.getBranchingPos()[group.getBranchingPos().length - 1] + 1,
                  mergeStrategy,
                  height
              ));
    }
  }

  @Override
  public ICNode getCDMChild(byte[] key, SearchStatus sts) {
    if (sts.getCurLen() == key.length) {
      int idx = getBrKeyIdx(0);
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
      return ptrs[getBrKeyIdx(0)];
    }

    int channel = getBrKeyIdx(bytes2Int(extractBytes(key, bps)));
    sts.setCurLen(checkKeyBytes(key, channel, bps));
    // sts.setFinished(sts.getCurLen() == key.length);
    return ptrs[channel];
  }

  @Override
  public void acceptInspector(NodeInspector noi) {
    noi.incEntry("CNode4_cnt", 1);
    noi.appendEntry("CNode4_valid_br", int2BytesNoTrailing(posInt).length);
    inspectRMK(noi, "CNode4");
  }

  private void setBranchingKeys(int[] collected) {
    bks = new int[collected.length];
    ptrs = new ICNode[collected.length];
    System.arraycopy(collected, 0, bks, 0, bks.length);
  }

  public void setBranchingKeys(List<Integer> branchingBytes) {
    ptrs = new ICNode[branchingBytes.size()];

    // init interleaved bytes array
    int[] itvPos = findIntervals(int2BytesNoTrailing(posInt));
    if (itvPos.length > 0) rmk = new byte[branchingBytes.size()][];

    bks = branchingBytes.stream().mapToInt(i -> i).toArray();
  }

  // get index of the target key
  private int getBrKeyIdx(int val) {
    int idx = Arrays.binarySearch(bks, val);
    if (idx >= 0 && bks[idx] != val) throw new RuntimeException("Key not found.");
    return idx;
  }

  private void setBranchingPtr(int idx, ICNode ptr) {
    ptrs[idx] = ptr;
  }

  @Override
  public byte[] assembleKeyAt(int pos) {
    byte[] res;
    byte[] brKey = int2Bytes(bks[pos]);
    brKey = removeTrailingZeros(brKey);

    int[] brPosInt = ICNode.unsignedByteArr2IntArr(int2BytesNoTrailing(posInt));
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
    ptrs[getBrKeyIdx(bytes2Int(k))] = (ICNode) n;
  }

  @Override
  public byte[][] getBranchingKeys() {
    byte[][] res = new byte[bks.length][];
    for (int i = 0; i < bks.length; i++) {
      res[i] = int2Bytes(bks[i]);
    }
    return res;
  }

  @Override
  public void replace(byte[] key, IMicroNode nNode) {
    setChild(key, nNode);
  }

  /** Adapted from public INode getChild(String name) { */
  @Override
  public IMicroNode getLogicalChild(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getValue() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<byte[]> getKeyBytes() {
    return Arrays.stream(bks).mapToObj(ByteEncode::int2BytesNoTrailing).collect(Collectors.toList());
  }

  @Override
  protected byte[] getBrKeyAt(int channel) {
    return int2BytesNoTrailing(bks[channel]);
  }

}
