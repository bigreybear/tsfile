package optimize.nodes.fdm.vfull;

import org.openjdk.jol.info.ClassLayout;

public class Node16Prefixed extends Node16 implements Prefixed{
  ISEARTNode prefixedPtr; // with no key

  public Node16Prefixed() {
    super();
  }

  public static void main(String[] args) {
    Node16Prefixed n = new Node16Prefixed();
    System.out.println(ClassLayout.parseInstance(n).toPrintable());
  }

  public ISEARTNode getPrefixedPtr() {
    return prefixedPtr;
  }
}
