package optimize.nodes.fdm;

import java.util.ArrayList;
import java.util.List;
import optimize.SearchStatus;
import optimize.nodes.IMicroNode;
import optimize.nodes.NodeInspector;
import optimize.nodes.NodeWithPartialKey;

public abstract class FNodeBase extends NodeWithPartialKey implements IFNode {

  protected abstract IFNode[] getPtrs();

  public List<IMicroNode> getChildren() {
    IFNode[] ptrs = getPtrs();
    List<IMicroNode> res = new ArrayList<>();
    for (int i = 0; i < ptrs.length; i++) {
      if (ptrs[i] != null) res.add(ptrs[i]);
    }
    return res;
  }

  // following methods are only for parallel key-ptr arrays
  protected int getPtrIdxByByte(final byte[] keys, byte k) {
    IFNode[] ptrs = getPtrs();
    int c = IFNode.binarySearchUnsignedByteArray(keys, 0, keys.length, k);
    return c >= 0 && ptrs[c] == null ? -c - 1 : c;
  }

  protected final void shiftInsert(int pos, byte kb, byte[] keys, IFNode child) {
    IFNode[] ptrs = getPtrs();
    System.arraycopy(keys, pos, keys, pos + 1, keys.length - pos - 1);
    System.arraycopy(ptrs, pos, ptrs, pos + 1, ptrs.length - pos - 1);
    keys[pos] = kb;
    ptrs[pos] = child;
  }

  public IFNode getFDMChild(final byte[] key, final SearchStatus sts) {
    if (sts.getCurLen() == key.length) {
      sts.setFinished(true);
      IFNode res = get((byte) 0);
      return res == null ? this : res;
    }

    int curLen = pk == null ? sts.getCurLen() : checkPartialKey(key, sts.getCurLen(), -1);
    if (curLen == key.length) {
      sts.setFinished(true);
      return get((byte) 0);
    }

    sts.setCurLen(curLen + 1);
    return get(key[curLen]);
  }

  protected int countValidPointers() {
    IFNode[] p = getPtrs();
    for (int i = 0, c = 0; ; ) {
      if (i == p.length) {
        return c;
      }
      if (p[i++] != null) c++;
    }
  }

  @Override
  public void acceptInspector(NodeInspector noi) {
    String c = getInspectCode();
    noi.incEntry(c + "_cnt", 1);
    if (getParKey() != null) noi.appendEntry(c + "_pk_len", getParKey().length);
    noi.appendEntry(c + "_dep", noi.getCurDepth());
    noi.appendEntry(c + "_val_ptr", countValidPointers());
  }

  protected abstract String getInspectCode();
}
