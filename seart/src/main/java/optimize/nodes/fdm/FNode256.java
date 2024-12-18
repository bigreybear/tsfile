package optimize.nodes.fdm;

import static optimize.nodes.fdm.vfull.SEARTNode.ubyte;
import static optimize.util.ArrayHelper.removeTrailingZeros;

import java.util.Arrays;
import java.util.List;
import optimize.nodes.IInternal;
import optimize.nodes.INode;

public class FNode256 implements IInternal, IFNode {
  // no prefixed key, but deem b\'00' as prefixed-pointer
  public byte[] pk;
  public INode[] ptrs;

  @Override
  public void setPartialKey(byte[] pk) {
    this.pk = pk;
  }

  @Override
  public void add(byte k, INode v) {
    ptrs[ubyte(k)] = v;
  }

  @Override
  public INode get(byte k) {
    return ptrs[ubyte(k)];
  }

  @Override
  public byte[] getKeysFromFDM() {
    byte[] res = new byte[256];
    for (int i = 0, len = 0; i < 256; i++) {
      if (ptrs[i] != null) res[len++] = (byte) i;
    }
    return removeTrailingZeros(res);
  }

  @Override
  public void replace(byte k, INode c) {
    ptrs[ubyte(k)] = c;
  }

  @Override
  public INode replace(byte[] k, INode c) {
    ptrs[ubyte(k[0])] = c;
    return this;
  }

  /** Copy and modify from {@linkplain Arrays#binarySearch}. */
  protected static int binarySearchUnsignedByteArray(
      byte[] a, int fromIndex, int toIndex, byte key) {
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

  protected int getPtrIdxByByte(byte[] keys, byte k) {
    int c = binarySearchUnsignedByteArray(keys, 0, keys.length, k);
    return c >= 0 && ptrs[c] == null ? -c - 1 : c;
  }

  protected final void shiftInsert(int pos, byte kb, byte[] keys, INode child) {
    System.arraycopy(keys, pos, keys, pos + 1, keys.length - pos - 1);
    System.arraycopy(ptrs, pos, ptrs, pos + 1, ptrs.length - pos - 1);
    keys[pos] = kb;
    ptrs[pos] = child;
  }

  public FNode256() {
    this.ptrs = new INode[256];
  }

  @Override
  public List<INode> getChildren() {
    return Arrays.asList(ptrs);
  }

  @Override
  public List<String> getKeys() {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] getPartialKey() {
    return pk;
  }

  @Override
  public long getValue() {
    return IInternal.super.getValue();
  }

  @Override
  public INode replace(String key, INode nNode) {
    return null;
  }
}
