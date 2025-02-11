package optimize.nodes.cdm.sorted;

import optimize.nodes.NodeInspector;
import optimize.nodes.cdm.ICNode;
import optimize.nodes.cdm.frame.CNode8;
import optimize.util.InfixGroup;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static optimize.nodes.cdm.ByteEncode.long2Bytes;
import static optimize.nodes.cdm.ByteEncode.long2BytesNoTrailing;

public class SCNode8 extends CNode8 {

  public SCNode8(int[] pos) {
    super(pos);
  }

  @Override
  protected int getKeyPos(long k) {
    return Arrays.binarySearch(bks, k);
  }

  @Override
  public List<byte[]> getKeyBytes() {
    List<byte[]> r = new ArrayList<>();
    for (long s : bks) {
      r.add(long2BytesNoTrailing(s));
    }
    return r;
  }

  @Override
  protected byte[] getBrKeyAt(int channel) {
    return long2BytesNoTrailing(bks[channel]);
  }

  @Override
  protected Object[] setBrKeysAndGenSupFunctions(InfixGroup group) {
    final long[] sbk = group.sortedLongBranchKeys();
    bks = new long[sbk.length];
    ptrs = new ICNode[sbk.length];
    System.arraycopy(sbk, 0, bks, 0, bks.length);
    Object[] res = new Object[2];
    res[0] = (Function<Integer, List<byte[]>>) (integer -> group.getCompleteKeys(long2Bytes(sbk[integer])));
    res[1] = (Function<Integer, Integer>) (i -> i);
    return res;
  }

  @Override
  protected String codeName() {
    return "SCNode8";
  }
}
