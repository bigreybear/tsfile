package optimize.nodes.fdm;

import optimize.SearchStatus;
import optimize.annotation.DebugOnly;
import optimize.exception.KeyNotFound;
import optimize.nodes.IMicroNode;
import optimize.nodes.NodeInspector;
import optimize.nodes.NodeWithPartialKey;
import optimize.nodes.logic.LLeaf;
import optimize.nodes.logic.LLeafAnnotated;

import java.util.List;

// Note(zx) this class is effectively not used, refers to its constructor.
public class FLeaf extends NodeWithPartialKey implements IFNode {
  public IFNode value;

  private FLeaf() {}

  @DebugOnly
  public static IFNode constructAnnotatedLLeaf(IFNode ptr, byte[] parKey, byte[] full) {
    // serve only two-phase merge
    LLeaf ol = (LLeaf) ptr;
    LLeafAnnotated lLeafAnnotated = new LLeafAnnotated(ol.getValue());
    lLeafAnnotated.getInfoObj().fullKey = full;
    if (parKey != null && parKey.length != 0) lLeafAnnotated.setParKey(parKey);
    return lLeafAnnotated;
  }

  public static IFNode constructFLeaf(IFNode ptr, byte[] parKey) {
    // to eliminate trivial leaf
    if (parKey == null || parKey.length == 0) return ptr;
    else {
      // FLeaf leaf = new FLeaf();
      // leaf.setParKey(parKey);
      // leaf.value = ptr;
      // return leaf;

      // Note(zx) omit trivial FLeaf, makes mutli-ART impossible
      if (ptr.getParKey() != null && ptr.getParKey().length != 0) throw new RuntimeException();
      ptr.setParKey(parKey);
      return ptr;
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
  public int getChildNum() {
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
  public void acceptInspector(NodeInspector noi) {
    noi.appendEntry("FLeaf_dep", noi.getCurDepth());
    noi.appendEntry("FLeaf_pk_len", getParKey().length);
  }

  protected String getInspectCode() {
    return "FLeaf";
  }

  @Override
  public List<IMicroNode> getChildren() {
    throw new UnsupportedOperationException();
  }
}
