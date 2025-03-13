package optimize.nodes.cdm.sorted;

import static optimize.nodes.cdm.ByteEncode.int2Bytes;
import static optimize.nodes.cdm.ByteEncode.int2BytesNoTrailing;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import optimize.IntegratedMain;
import optimize.nodes.cdm.ByteEncode;
import optimize.nodes.cdm.ICNode;
import optimize.nodes.cdm.frame.CNode4;
import optimize.util.ByteArray;
import optimize.util.InfixGroup;
import optimize.util.InternalInspector;

public class SCNode4 extends CNode4 {
  public SCNode4(int[] pos) {
    super(pos);
  }

  @Override
  protected void setBranchKeyValRmk(Map<ByteArray, ICNode> m, Map<ByteArray, byte[]> k2r, int actualSize) {
    int size = actualSize, ttlRmkLen = 0, curIdx = 0;
    bks = new int[size];

    int k;
    byte[] curRmk;
    Map<Integer, ByteArray> remap = new HashMap<>();

    int idx = 0;
    for (ByteArray ba : m.keySet()) {
      k = ByteEncode.bytes2Int(ba.getVal());
      remap.put(k, ba);
      bks[idx++] = k;
    }
    Arrays.sort(bks);

    for (int i = 0; i < bks.length; i++) {
      ByteArray bak = remap.get(bks[i]);
      ptrs[i] = m.get(bak);
      curRmk = k2r.get(bak);
      rmk[i] = curRmk.length == 0 ? null : curRmk;
      ttlRmkLen += curRmk.length;
    }

    if (ttlRmkLen == 0) {
      rmk = null;
    }
  }

  @Override
  protected int getKeyPos(int k) {
    if (IntegratedMain.INTERNAL_PROFILE) {
      long watch = System.nanoTime();
      int res = Arrays.binarySearch(bks, k);
      watch = System.nanoTime() - watch;
      InternalInspector.appendEntry( codeName() + "_query_time", watch);
      return res;
    } else {
      return Arrays.binarySearch(bks, k);
    }
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
