package optimize.nodes.fdm.vfull;

import optimize.nodes.INode;

import java.nio.charset.StandardCharsets;

public class Leaf implements ISEARTNode {
  byte[] partialKey;
  INode value;

  public Leaf(byte[] pk, INode val) {
    partialKey = pk;
    value = val;
  }

  public Leaf() {}

  @Override
  public final boolean isLeaf() {
    return true;
  }

  @Override
  public final byte[] getPartialKey() {
    return partialKey;
  }

  @Override
  public INode getValue() {
    return value;
  }

  public void setValue(INode val) {
    value = val;
  }

  @Override
  public final void reassignPartialKey(byte[] pk) {
    partialKey = pk;
  }

  @Override
  public String toString() {
    return String.format(" %s : %d", new String(partialKey, StandardCharsets.UTF_8), value.getPartialKey());
  }

  // endregion

  public static void main(String[] args) {
    Leaf a = new Leaf();
    System.out.println(a.value);
  }
}
