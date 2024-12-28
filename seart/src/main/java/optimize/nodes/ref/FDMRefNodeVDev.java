package optimize.nodes.ref;

import java.util.Arrays;
import java.util.List;
import optimize.SearchStatus;
import optimize.nodes.IMicroNode;
import optimize.nodes.NodeWithPartialKey;
import optimize.nodes.fdm.FLeaf;
import optimize.nodes.fdm.FNode256;
import optimize.nodes.fdm.IFNode;
import optimize.nodes.logic.LLeaf;

public class FDMRefNodeVDev extends NodeWithPartialKey implements IFNode {
  public IFNode template;
  public long[] values;

  public void embedTemplate(IFNode ori, IFNode t) {
    pk = ori.getParKey();
    byte[] keys = ori.getKeysFromFDM();
    values = new long[keys.length];
    template = t;
    for (byte k : keys) {
      long oriValue = ((FLeaf) ori.get(k)).getFValue().getValue();
      int order = (int) t.get(k).getValue();
      values[order] = oriValue;
    }
  }

  public static IFNode buildFDMTemplate(IFNode node) {
    byte[] keys = node.getKeysFromFDM();
    Arrays.sort(keys);
    LLeaf leaf;
    IFNode tr = new FNode256();
    for (int i = 0; i < keys.length; i++) {
      leaf = new LLeaf(i);
      leaf.setParKey(node.get(keys[i]).getParKey());
      tr.add(keys[i], leaf);
    }
    return tr;
  }

  public long getValFrom(byte k) {
    return values[(int) template.get(k).getValue()];
  }

  public long getValFrom(byte[] k, int _i) {
    int idx = _i;
    if (pk != null) {
      for (int i = 0; i < pk.length && idx < k.length; i++, idx++) {
        if (pk[i] != k[idx]) break;
      }
    }

    byte tar = (idx >= k.length) ? 0 : k[idx];
    return values[(int) template.get(tar).getValue()];
  }

  @Override
  public List<IMicroNode> getChildren() {
    throw new UnsupportedOperationException();
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
    // todo could be impl.
    return null;
  }

  @Override
  public void replace(byte k, IFNode n) {
    throw new UnsupportedOperationException();
  }

  @Override
  public IFNode getFDMChild(byte[] key, SearchStatus sts) {
    // todo
    throw new UnsupportedOperationException();
  }
}
