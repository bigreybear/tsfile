package optimize.nodes.cdm.sorted;

import optimize.IntegratedMain;
import optimize.annotation.DebugOnly;
import optimize.nodes.cdm.ByteEncode;
import optimize.nodes.cdm.ICNode;
import optimize.nodes.cdm.frame.CNode8;
import optimize.util.ByteArray;
import optimize.util.InfixGroup;
import optimize.util.InternalInspector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static optimize.nodes.cdm.ByteEncode.long2Bytes;
import static optimize.nodes.cdm.ByteEncode.long2BytesNoTrailing;

public class SCNode8 extends CNode8 {

  public SCNode8(int[] pos) {
    super(pos);
  }

  @Override
  protected void setBranchKeyValRmk(Map<ByteArray, ICNode> m, Map<ByteArray, byte[]> k2r) {
    int size = m.size(), ttlRmkLen = 0, curIdx = 0;
    bks = new long[size];

    long k;
    byte[] curRmk;
    Map<Long, ByteArray> remap = new HashMap<>();

    int idx = 0;
    for (ByteArray ba : m.keySet()) {
      k = ByteEncode.bytes2Long(ba.getVal());
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
  protected int getKeyPos(long k) {
    if (IntegratedMain.INTERNAL_PROFILE) {
      @DebugOnly
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
