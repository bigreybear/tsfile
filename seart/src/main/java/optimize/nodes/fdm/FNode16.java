package optimize.nodes.fdm;

import java.util.Arrays;
import optimize.nodes.INode;

public class FNode16 extends FNode256 {
  public byte[] keys;

  public FNode16() {
    this.keys = new byte[16];
    this.ptrs = new INode[16];
    Arrays.fill(keys, (byte) 0xff);
  }

  @Override
  public void add(byte k, INode v) {
    int pos = getPtrIdxByByte(keys, k);
    shiftInsert(-pos - 1, k, keys, v);
  }

  @Override
  public INode get(byte k) {
    return ptrs[getPtrIdxByByte(keys, k)];
  }
}
