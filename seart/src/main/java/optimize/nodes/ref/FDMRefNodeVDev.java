package optimize.nodes.ref;

import java.util.Arrays;
import java.util.List;
import optimize.nodes.INode;
import optimize.nodes.fdm.FLeaf;
import optimize.nodes.fdm.FNode256;
import optimize.nodes.fdm.IFNode;
import optimize.nodes.logic.LLeaf;

public class FDMRefNodeVDev implements INode {
  public byte[] pk;
  public IFNode template;
  public long[] values;

  public void embedTemplate(IFNode ori, IFNode t) {
    pk = ori.getPartialKey();
    byte[] keys = ori.getKeysFromFDM();
    values = new long[keys.length];
    template = t;
    for (byte k : keys) {
      long oriValue = ((FLeaf) ori.get(k)).getFValue().getValue();
      int order = (int) t.get(k).getValue();
      values[order] = oriValue;
    }
  }

  public static INode buildFDMTemplate(IFNode node) {
    byte[] keys = node.getKeysFromFDM();
    Arrays.sort(keys);
    LLeaf leaf;
    IFNode tr = new FNode256();
    for (int i = 0; i < keys.length; i++) {
      leaf = new LLeaf(i);
      leaf.pk = node.get(keys[i]).getPartialKey();
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
  public long getValue() {
    return 0;
  }

  @Override
  public INode getChild(String name) {
    return null;
  }

  @Override
  public List<INode> getChildren() {
    return null;
  }

  @Override
  public List<String> getKeys() {
    return null;
  }

  @Override
  public byte[] getPartialKey() {
    return new byte[0];
  }

  @Override
  public INode addChild(String name, INode child) {
    return null;
  }

  @Override
  public INode replace(String key, INode nNode) {
    return null;
  }
}
