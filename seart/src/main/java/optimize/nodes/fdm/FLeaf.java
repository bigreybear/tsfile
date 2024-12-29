package optimize.nodes.fdm;

import optimize.SearchStatus;
import optimize.exception.KeyNotFound;
import optimize.nodes.NodeInspector;

public class FLeaf extends FNodeBase {
  public IFNode value;

  private FLeaf() {}

  public static IFNode constructFLeaf(IFNode ptr, byte[] parKey) {
    // to eliminate trivial leaf
    if (parKey == null || parKey.length == 0) return ptr;
    else {
      FLeaf leaf = new FLeaf();
      leaf.setParKey(parKey);
      leaf.value = ptr;
      return leaf;
    }
  }

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

  @Override
  public IFNode getFDMChild(byte[] key, SearchStatus sts) {
    if (sts.getCurLen() == key.length) {
      sts.setFinished(true);
      return this;
    }

    if (key.length == checkPartialKey(key, sts.getCurLen(), -1)) {
      sts.setFinished(true);
      return value;
    }
    throw new KeyNotFound(key);
  }

  @Override
  protected IFNode[] getPtrs() {
    return new IFNode[] {value};
  }

  @Override
  public void acceptInspector(NodeInspector noi) {
    noi.appendEntry("FLeaf_dep", noi.getCurDepth());
    noi.appendEntry("FLeaf_pk_len", getParKey().length);
  }

  @Override
  protected String getInspectCode() {
    return "FLeaf";
  }
}
