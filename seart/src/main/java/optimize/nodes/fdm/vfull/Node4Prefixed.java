package optimize.nodes.fdm.vfull;

public class Node4Prefixed extends Node4 implements Prefixed {
  ISEARTNode prefixedPtr; // with no key

  public Node4Prefixed(byte[] pk, byte k1, ISEARTNode p1, ISEARTNode prefixedPtr) {
    partialKey = pk;
    keys[0] = k1;
    ptrs[0] = p1;
    this.prefixedPtr = prefixedPtr;
  }

  public Node4Prefixed() {
    super();
  }

  public ISEARTNode getPrefixedPtr() {
    return prefixedPtr;
  }
}
