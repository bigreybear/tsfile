package optimize.nodes.fdm;

import optimize.nodes.INode;

// todo eliminate this class
public class FLeaf extends FNodeBase implements IFNode {
  public IFNode value;

  @Override
  public void add(byte k, IFNode v) {
    throw new UnsupportedOperationException();
  }

  @Override
  public IFNode get(byte k) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] getKeysFromFDM() {
    throw new UnsupportedOperationException();
  }

  @Override
  public IFNode getFValue() {
    return value;
  }

  public void setValue(IFNode v) {
    value = v;
  }

  @Override
  public void replace(byte k, IFNode c) {
    throw new UnsupportedOperationException();
  }
}
