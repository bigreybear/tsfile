package optimize.nodes.cdm.hashed;

import optimize.IntegratedMain;
import optimize.annotation.DebugOnly;
import optimize.nodes.cdm.ByteEncode;
import optimize.nodes.cdm.ICNode;
import optimize.nodes.cdm.frame.CNode4;
import optimize.util.InfixGroup;
import optimize.util.InternalInspector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static optimize.IntegratedMain.INTERNAL_PROFILE;
import static optimize.nodes.cdm.ByteEncode.int2Bytes;

public class HCNode4 extends CNode4 {

  public HCNode4(int[] pos) {
    super(pos);
  }

  @Override
  protected Object[] setBrKeysAndGenSupFunctions(InfixGroup group) {
    final int[] sbk = group.sortedIntBranchKeys();
    int len = sbk.length;
    len += (sbk.length & 0b1) == 0 ? 0 : 1;
    bks = new int[len];
    ptrs = new ICNode[len];

    final int[] map = new int[len];
    Arrays.fill(map, -1);

    int hid, hash;
    BitSet bs = new BitSet(len);
    for (int idx = 0, sbkLen = sbk.length; idx < sbkLen; idx++){
      hash = HashHelper.hash1(sbk[idx]);

      while (bs.get(hid = hash % len)) {
        hash = HashHelper.rehash(hash, sbk[idx]);
      }

      bs.set(hid);
      bks[hid] = sbk[idx];
      map[idx] = hid;
    }

    Object[] res = new Object[2];
    res[0] = (Function<Integer, List<byte[]>>) (integer -> group.getCompleteKeys(int2Bytes(sbk[integer])));
    res[1] = (Function<Integer, Integer>) (i -> map[i]);
    return res;
  }

  @Override
  public List<byte[]> getKeyBytes() {
    List<Integer> sbk = new ArrayList<>();
    for (int i = 0, len = bks.length; i < len; i++) {
      if (ptrs[i] != null) sbk.add(bks[i]);
    }
    Collections.sort(sbk);
    return sbk.stream().map(ByteEncode::int2BytesNoTrailing).collect(Collectors.toList());
  }

  @Override
  protected int getKeyPos(int k) {
    if (INTERNAL_PROFILE) {
      long watch = System.nanoTime();
      int rehashCnt = 0;

      int hash = HashHelper.hash1(k);
      int pos = hash % bks.length;
      while (bks[pos] != k || ptrs[pos] == null) {
        hash = HashHelper.rehash(hash, k);
        pos = hash % bks.length;
        rehashCnt++;
      }
      watch = System.nanoTime() - watch;
      InternalInspector.appendEntry(codeName() + "_rehash_cnt", rehashCnt);
      InternalInspector.appendEntry( codeName() + "_query_time", watch);
      return pos;
    } else {
      int hash = HashHelper.hash1(k);
      int pos = hash % bks.length;
      while (bks[pos] != k || ptrs[pos] == null) {
        hash = HashHelper.rehash(hash, k);
        pos = hash % bks.length;
      }
      return pos;
    }
  }

  @Override
  protected byte[] getBrKeyAt(int channel) {
    return getKeyBytes().get(channel);
  }

  @Override
  protected String codeName() {
    return "HCNode4";
  }
}
