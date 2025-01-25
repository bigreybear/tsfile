package optimize.nodes.cdm;

import static optimize.nodes.cdm.ByteEncode.bytes2Int;
import static optimize.nodes.cdm.ByteEncode.int2Bytes;
import static optimize.nodes.cdm.ByteEncode.int2BytesNoTrailing;
import static optimize.nodes.cdm.CNodeHelper.setBytesByPosNoCheck;
import static optimize.util.ArrayHelper.findIntervals;
import static optimize.util.ArrayHelper.removeTrailingZeros;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import optimize.nodes.IMicroNode;
import optimize.nodes.NodeInspector;
import optimize.util.InfixGroup;

public class SCNode4 extends SortedCNodeBase {
  // for only 4 positions
  final byte p1, p2, p3, p4;
  int[] bks; // indeed a byte[][4] bks; // branching keys

  // exactly no padding on 64-jvm, jdk-17, Compressed OOPs

  // raw keys might with prefix
  public SCNode4(int[] pos) {
    if (pos.length > 4)
      throw new UnsupportedOperationException("No more than 4 bytes branching key yet.");

    // pos int init.
    for (int p : pos) {
      if ((p & SINGLE_BYTE_MASK) != 0) throw new RuntimeException("Invalid Branch Pos.");
    }

    p1 = (byte) (pos[0] & 0xff);
    p2 = pos.length > 1 ? (byte) (pos[1] & 0xff) : 0;
    p3 = (pos.length > 2 && p2 != 0) ? (byte) (pos[2] & 0xff) : 0;
    p4 = (pos.length > 3 && p3 != 0) ? (byte) (pos[3] & 0xff) : 0;
  }

  @Override
  public int[] getBranchingPos() {
    if (p1 == 0) throw new RuntimeException("Invalid branch pos for CNode8");
    if (p2 == 0) return new int[] { p1 & 0xff };
    if (p3 == 0) return new int[] { p1 & 0xff, p2 & 0xff };
    if (p4 == 0) return new int[] { p1 & 0xff, p2 & 0xff, p3 & 0xff };
    return new int[] { p1 & 0xff, p2 & 0xff, p3 & 0xff, p4 & 0xff};
  }

  // a support method for setContent
  @Override
  protected final Function<Integer, List<byte[]>> generateCompleteKeyRetrieval(InfixGroup group) {
    final int[] sbk = group.sortedIntBranchKeys();
    setBranchingKeys(sbk);
    return (integer -> group.getCompleteKeys(int2Bytes(sbk[integer])));
  }

  @Override
  protected String codeName() {
    return "CNode4";
  }

  @Override
  public void acceptInspector(NodeInspector noi) {
    super.acceptInspector(noi);
    noi.incEntry("CNode4_cnt", 1);
  }

  private void setBranchingKeys(int[] collected) {
    bks = new int[collected.length];
    ptrs = new ICNode[collected.length];
    System.arraycopy(collected, 0, bks, 0, bks.length);
  }

  public void setBranchingKeys(List<Integer> branchingBytes) {
    ptrs = new ICNode[branchingBytes.size()];

    // init interleaved bytes array
    int[] itvPos = findIntervals(p1, p2, p3, p4);
    if (itvPos.length > 0) rmk = new byte[branchingBytes.size()][];

    bks = branchingBytes.stream().mapToInt(i -> i).toArray();
  }

  @Override
  protected int getEmptyKeyIdx() {
    int idx = Arrays.binarySearch(bks, 0);
    if (idx < 0 || bks[idx] != 0) throw new RuntimeException("Empty key not found.");
    return idx;
  }

  @Override
  protected int getBrKeyIdx(byte[] key, int[] brPos) {
    // brPos has no trailing zeros.
    int len = brPos.length;
    if (len > 4)
      throw new UnsupportedOperationException("5 or more bytes cannot be encoded to an int.");
    int sk = 0;
    switch (len) {
      case 4:
        sk |= brPos[3] < key.length ? (key[brPos[3]] & 0xFF) : 0;
      case 3:
        sk |= brPos[2] < key.length ? (key[brPos[2]] & 0xFF) << 8 : 0;
      case 2:
        sk |= brPos[1] < key.length ? (key[brPos[1]] & 0xFF) << 16 : 0;
      case 1:
        sk |= (key[brPos[0]] & 0xFF) << 24;
        break;
      default:
        throw new UnsupportedOperationException();
    }
    int idx = Arrays.binarySearch(bks, sk);
    if (idx < 0 || bks[idx] != sk) throw new RuntimeException("Key not found.");
    return idx;
  }

  // get index of the target key
  private int getBrKeyIdx(int val) {
    int idx = Arrays.binarySearch(bks, val);
    if (idx < 0 || bks[idx] != val) throw new RuntimeException("Key not found.");
    return idx;
  }

  @Override
  public byte[] assembleKeyAt(int pos) {
    byte[] res;
    byte[] brKey = int2Bytes(bks[pos]);
    brKey = removeTrailingZeros(brKey);

    int[] brPosInt = byteArr2IntArr(removeTrailingZeros(p1,p2,p3,p4));
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
  public byte[][] getBranchingKeys() {
    byte[][] res = new byte[bks.length][];
    for (int i = 0; i < bks.length; i++) {
      res[i] = int2Bytes(bks[i]);
    }
    return res;
  }

  @Override
  public long getValue() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<byte[]> getKeyBytes() {
    return Arrays.stream(bks)
        .mapToObj(ByteEncode::int2BytesNoTrailing)
        .collect(Collectors.toList());
  }

  @Override
  protected byte[] getBrKeyAt(int channel) {
    return int2BytesNoTrailing(bks[channel]);
  }
}
