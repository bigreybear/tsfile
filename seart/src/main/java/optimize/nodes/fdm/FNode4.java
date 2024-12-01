package optimize.nodes.fdm;

import optimize.nodes.INode;

import java.util.Arrays;

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
    shiftInsert(-pos-1, k, keys,v);
  }

  @Override
  public INode get(byte k) {
    return ptrs[getPtrIdxByByte(keys, k)];
  }
}
