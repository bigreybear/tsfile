package optimize.nodes.fdm.vfull;

public class Node48Prefixed extends Node48 implements Prefixed {
  ISEARTNode prefixedPtr; // with no key

  public Node48Prefixed() {
    super();
  }

  public ISEARTNode getPrefixedPtr() {
    return prefixedPtr;
  }
}
