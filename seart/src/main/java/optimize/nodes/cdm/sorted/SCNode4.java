package optimize.nodes.cdm.sorted;

import static optimize.nodes.cdm.ByteEncode.int2Bytes;
import static optimize.nodes.cdm.ByteEncode.int2BytesNoTrailing;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import optimize.nodes.NodeInspector;
import optimize.nodes.cdm.ByteEncode;
import optimize.nodes.cdm.ICNode;
import optimize.nodes.cdm.frame.CNode4;
import optimize.util.InfixGroup;

public class SCNode4 extends CNode4 {
  public SCNode4(int[] pos) {
    super(pos);
  }

  @Override
  protected int getKeyPos(int k) {
    return Arrays.binarySearch(bks, k);
  }

  @Override
  protected String codeName() {
    return "SCNode4";
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

  @Override
  protected Object[] setBrKeysAndGenSupFunctions(InfixGroup group) {
    final int[] sbk = group.sortedIntBranchKeys();
    bks = new int[sbk.length];
    ptrs = new ICNode[sbk.length];
    System.arraycopy(sbk, 0, bks, 0, bks.length);
    Object[] res = new Object[2];
    res[0] = (Function<Integer, List<byte[]>>) (integer -> group.getCompleteKeys(int2Bytes(sbk[integer])));
    res[1] = (Function<Integer, Integer>) (i -> i);
    return res;
  }

  // region For Suffix
  // @Override
  // public byte[] assembleKeyAt(int pos) {
  //   byte[] res;
  //   byte[] brKey = int2Bytes(bks[pos]);
  //   brKey = removeTrailingZeros(brKey);
  //
  //   int[] brPosInt = byteArr2IntArr(removeTrailingZeros(p1,p2,p3,p4));
  //   int[] itvPosInt = findIntervals(brPosInt);
  //
  //   int keyLen = brPosInt[brPosInt.length - 1] - brPosInt[0] + 1;
  //
  //   int[] brRltPos = ICNode.shiftIntArr(brPosInt, -1 * brPosInt[0]);
  //   int[] itvRltPos = ICNode.shiftIntArr(itvPosInt, -1 * brPosInt[0]);
  //
  //   byte[] asmkey = new byte[keyLen];
  //   // ICNode.setBytesByPos(asmkey, brKey, brRltPos);
  //   setBytesByPosNoCheck(asmkey, brKey, brRltPos);
  //   if (rmk != null && rmk[pos] != null) setBytesByPosNoCheck(asmkey, rmk[pos], itvRltPos);
  //   // ICNode.setBytesByPos(asmkey, rmk[pos], itvRltPos);
  //   return removeTrailingZeros(asmkey);
  // }
  //
  // @Override
  // public IMicroNode getChild(byte[] k) {
  //   if (k.length > 4) throw new UnsupportedOperationException();
  //   int ans = bytes2Int(k);
  //   int idx1 = getKeyPos(ans);
  //   if (idx1 < 0 || bks[idx1] != ans) throw new RuntimeException("Key not found.");
  //   int idx = idx1;
  //   return ptrs[idx];
  // }
  // @Override
  // public byte[][] getBranchingKeys() {
  //   byte[][] res = new byte[bks.length][];
  //   for (int i = 0; i < bks.length; i++) {
  //     res[i] = int2Bytes(bks[i]);
  //   }
  //   return res;
  // }
  // endregion
}
