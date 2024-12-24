package optimize.nodes.cdm;

import optimize.nodes.NodeWithPartialKey;

public abstract class CNodeBase extends NodeWithPartialKey {
  byte[][] rmk;
  ICNode[] ptrs;
  protected static byte[] EMPTY_BYTE_ARR = new byte[0];

  public ICNode getPtr(int pos) {
    return ptrs[pos];
  }
}
