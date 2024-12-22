package optimize.nodes.fdm;

import optimize.nodes.IMicroNode;
import optimize.nodes.NodeWithPartialKey;

import java.util.ArrayList;
import java.util.List;

public abstract class FNodeBase extends NodeWithPartialKey {
  protected IFNode[] ptrs;

  public List<IMicroNode> getChildren() {
    List<IMicroNode> res = new ArrayList<>();
    for (int i = 0; i < ptrs.length; i++) {
      if (ptrs[i] != null) res.add(ptrs[i]);
    }
    return res;
  }

  // following methods are only for parallel key-ptr arrays
  protected int getPtrIdxByByte(byte[] keys, byte k) {
    int c = IFNode.binarySearchUnsignedByteArray(keys, 0, keys.length, k);
    return c >= 0 && ptrs[c] == null ? -c - 1 : c;
  }

  protected final void shiftInsert(int pos, byte kb, byte[] keys, IFNode child) {
    System.arraycopy(keys, pos, keys, pos + 1, keys.length - pos - 1);
    System.arraycopy(ptrs, pos, ptrs, pos + 1, ptrs.length - pos - 1);
    keys[pos] = kb;
    ptrs[pos] = child;
  }
}
