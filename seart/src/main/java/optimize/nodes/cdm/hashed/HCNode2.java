package optimize.nodes.cdm.hashed;

import optimize.annotation.DebugOnly;
import optimize.nodes.cdm.ByteEncode;
import optimize.nodes.cdm.ICNode;
import optimize.nodes.cdm.frame.CNode2;
import optimize.util.InfixGroup;
import optimize.util.InternalInspector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static optimize.nodes.cdm.ByteEncode.short2Bytes;

public class HCNode2 extends CNode2 {

  public HCNode2(int[] pos) {
    super(pos);
  }

  @Override
  protected Object[] setBrKeysAndGenSupFunctions(InfixGroup group) {
    final short[] sbk = group.sortedShortBranchKeys();
    int len = sbk.length;
    len += (sbk.length & 0b11) == 0 ? 0 : 4 - (sbk.length & 0b11);
    bks = new short[len];
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
    res[0] = (Function<Integer, List<byte[]>>) (integer -> group.getCompleteKeys(short2Bytes(sbk[integer])));
    res[1] = (Function<Integer, Integer>) (i -> map[i]);
    return res;
  }

  @Override
  public List<byte[]> getKeyBytes() {
    List<Short> sbk = new ArrayList<>();
    for (int i = 0, len = bks.length; i < len; i++) {
      if (ptrs[i] != null) sbk.add(bks[i]);
    }
    Collections.sort(sbk);
    return sbk.stream().map(ByteEncode::short2BytesNoTrailing).collect(Collectors.toList());
  }

  @Override
  protected int getKeyPos(short k) {
    int hash = HashHelper.hash1(k);
    int pos = hash % bks.length;

    @DebugOnly
    int rehashCnt = 0;
    while (bks[pos] != k || ptrs[pos] == null) {
      hash = HashHelper.rehash(hash, k);
      pos = hash % bks.length;
      if (INTERNAL_PROFILE) rehashCnt++;
    }
    if (INTERNAL_PROFILE) InternalInspector.appendEntry(codeName() + "_rehash_cnt", rehashCnt);
    return pos;
  }

  @Override
  protected byte[] getBrKeyAt(int channel) {
    return getKeyBytes().get(channel);
  }

  @Override
  protected String codeName() {
    return "HCNode2";
  }
}
