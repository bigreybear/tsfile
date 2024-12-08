package optimize.nodes.fdm;

import java.nio.charset.StandardCharsets;
import java.util.List;
import optimize.nodes.INode;

public interface IFNode extends INode {

  void add(byte k, INode v);

  INode get(byte k);

  default void replace(byte k, INode c) {}

  void setPartialKey(byte[] pk);

  default void setValue(INode v) {
    throw new UnsupportedOperationException();
  }

  default INode getFValue() {
    return null;
  }

  @Override
  default long getValue() {
    return 0;
  }

  @Override
  default INode getChild(String name) {
    IFNode cur = this;
    byte[] kbs = name.getBytes(StandardCharsets.UTF_8);
    byte[] pk = getPartialKey();
    for (int i = 0; i < kbs.length; ) {
      i += matchLen(pk, kbs, i);

      if (i < kbs.length) {
        cur = (IFNode) cur.get(kbs[i]);
        pk = cur.getPartialKey();
        i++;
      }
    }

    return cur instanceof FLeaf ? cur.getFValue() : ((IFNode) cur.get((byte) 0)).getFValue();
  }

  static int matchLen(byte[] pk, byte[] ik, int ofs) {
    if (pk == null) {
      return 0;
    }

    int pi = 0, ii = ofs;
    for (; pi < pk.length && ii < ik.length; ii++, pi++) {
      if (pk[pi] != ik[ii]) break;
    }

    return pi;
  }

  @Override
  default List<INode> getChildren() {
    return null;
  }

  @Override
  default List<String> getKeys() {
    return null;
  }

  @Override
  default INode addChild(String name, INode child) {
    return null;
  }

  @Override
  default INode replace(String key, INode nNode) {
    return null;
  }
}
