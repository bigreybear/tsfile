package optimize.nodes.fdm.vfull;

public class Node256Prefixed extends Node256 implements Prefixed {
  ISEARTNode prefixedPtr; // with no key

  public Node256Prefixed() {
    super();
  }

  public ISEARTNode getPrefixedPtr() {
    return prefixedPtr;
  }
}
