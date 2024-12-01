package optimize.nodes.fdm;

import optimize.nodes.INode;

public class FLeaf implements IFNode{
  byte[] pk;
  public INode value;

  @Override
  public void add(byte k, INode v) {
    throw new UnsupportedOperationException();
  }

  @Override
  public INode get(byte k) {
    return null;
  }

  @Override
  public INode getFValue() {
    return value;
  }

  @Override
  public void setValue(INode v) {
    value = v;
  }

  @Override
  public void setPartialKey(byte[] pk) {
    this.pk = pk;
  }

  @Override
  public byte[] getPartialKey() {
    return pk;
  }
}
