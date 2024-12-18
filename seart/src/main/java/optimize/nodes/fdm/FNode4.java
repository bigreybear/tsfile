package optimize.nodes.fdm;

import java.util.Arrays;
import optimize.nodes.INode;
import optimize.util.ArrayHelper;

public class FNode4 extends FNode256 {
  public byte[] keys;

  public FNode4() {
    this.keys = new byte[4];
    this.ptrs = new INode[4];
    Arrays.fill(keys, (byte) 0xff);
  }

  @Override
  public void add(byte k, INode v) {
    int pos = getPtrIdxByByte(keys, k);
    shiftInsert(-pos - 1, k, keys, v);
  }

  @Override
  public INode get(byte k) {
    int idx = getPtrIdxByByte(keys, k);
    if (idx < 0) return null;
    return ptrs[getPtrIdxByByte(keys, k)];
  }

  @Override
  public byte[] getKeysFromFDM() {
    return ArrayHelper.removeTrailing(keys, (byte) 0xff);
  }

  @Override
  public void replace(byte k, INode c) {
    for (int i = 0; i < keys.length && ptrs[i] != null; i++) {
      if (keys[i] == k) ptrs[i] = c;
    }
  }

  @Override
  public INode replace(byte[] k, INode c) {
    for (int i = 0; i < keys.length && ptrs[i] != null; i++) {
      if (keys[i] == k[0]) ptrs[i] = c;
    }
    return null;
  }
}
