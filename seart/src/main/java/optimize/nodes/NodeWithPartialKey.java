package optimize.nodes;

public abstract class NodeWithPartialKey {
  protected byte[] pk;

  public byte[] getParKey() {return pk;}

  public void setParKey(byte[] _pk) {pk = _pk;}
}
