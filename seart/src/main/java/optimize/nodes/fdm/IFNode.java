package optimize.nodes.fdm;


import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import optimize.SearchStatus;
import optimize.nodes.IMicroNode;
import optimize.nodes.ITSNode;

import static optimize.nodes.fdm.FNodeBase.ubyte;

public interface IFNode extends IMicroNode {

  IFNode getFDMChild(final byte[] key, final SearchStatus sts);

  /** Copy and modify from {@linkplain Arrays#binarySearch}. */
  static int binarySearchUnsignedByteArray(byte[] a, int fromIndex, int toIndex, byte key) {
    int low = fromIndex;
    int high = toIndex - 1;
    int sk = ubyte(key);

    while (low <= high) {
      int mid = (low + high) >>> 1;
      int midVal = ubyte(a[mid]);

      if (midVal < sk) low = mid + 1;
      else if (midVal > sk) high = mid - 1;
      else return mid; // key found
    }
    return -(low + 1); // key not found.
  }

  void add(byte k, IFNode v);

  @Override
  default void setChild(byte[] k, IMicroNode n) {
    add(k[0], (IFNode) n);
  }

  IFNode get(byte k);

  default IMicroNode getChild(byte[] k) {
    return get(k[0]);
  }

  byte[] getKeysFromFDM();

  default List<byte[]> getKeyBytes() {
    byte[] r = getKeysFromFDM();
    List<byte[]> res = new ArrayList<>();
    for (int i = 0; i < r.length; i++) {
      res.add(new byte[] {r[i]});
    }
    return res;
  }

  void replace(byte k, IFNode n);

  default void replace(byte[] key, IMicroNode node) {
    replace(key[0], (IFNode) node);
  }

  @Override
  default long getValue() {
    throw new UnsupportedOperationException();
  }

  default IFNode getFValue() {
    throw new UnsupportedOperationException();
  }

  @Override
  default ITSNode getLogicalChild(String name) {
    IFNode cur = this;
    byte[] kbs = name.getBytes(StandardCharsets.UTF_8);
    byte[] pk = getParKey();
    for (int i = 0; i < kbs.length; ) {
      i += matchLen(pk, kbs, i);

      if (i < kbs.length) {
        cur = cur.get(kbs[i]);
        pk = cur.getParKey();
        i++;
      }
    }

    IFNode res = cur.get((byte) 0);
    return res == null ? cur : res;
    // return cur instanceof FLeaf ? cur.getFValue() : ((IFNode) cur.get((byte) 0)).getFValue();
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
}
