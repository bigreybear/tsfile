package mtree;

public class MLeaf implements IMNode{
  long value;

  public MLeaf() {

  }

  public MLeaf(long val) {
    value = val;
  }

  @Override
  public long getValue() {
    return value;
  }

  @Override
  public IMNode getChild(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addChild(String name, IMNode child) {
    throw new UnsupportedOperationException();
  }
}
