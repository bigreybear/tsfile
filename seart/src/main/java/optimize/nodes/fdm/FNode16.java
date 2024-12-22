package optimize.nodes.fdm;

import java.util.Arrays;
import optimize.nodes.INode;
import optimize.util.ArrayHelper;

public class FNode16 extends FNodeBase implements IFNode {
  public byte[] keys;

  public FNode16() {
    this.keys = new byte[16];
    this.ptrs = new IFNode[16];
    Arrays.fill(keys, (byte) 0xff);
  }

  @Override
  public void add(byte k, IFNode v) {
    int pos = getPtrIdxByByte(keys, k);
    shiftInsert(-pos - 1, k, keys, v);
  }

  @Override
  public IFNode get(byte k) {
    int idx = getPtrIdxByByte(keys, k);
    if (idx < 0) return null;
    return ptrs[getPtrIdxByByte(keys, k)];
  }

  @Override
  public byte[] getKeysFromFDM() {
    return ArrayHelper.removeTrailing(keys, (byte) 0xff);
  }

  @Override
  public void replace(byte k, IFNode c) {
    for (int i = 0; i < keys.length && ptrs[i] != null; i++) {
      if (keys[i] == k) ptrs[i] = c;
    }
  }
}
