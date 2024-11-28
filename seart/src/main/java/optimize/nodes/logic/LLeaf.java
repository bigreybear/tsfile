package optimize.nodes.logic;

import optimize.nodes.ILeaf;

public class LLeaf implements ILeaf {
  byte[] pk;
  long value;

  public LLeaf() {}

  public LLeaf(long val) {
    value = val;
  }

  @Override
  public long getValue() {
    return value;
  }

  @Override
  public byte[] getPartialKey() {
    return pk;
  }
}
