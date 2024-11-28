package optimize.nodes.fdm.vfull;

import java.util.Arrays;

public class Node16 extends SEARTNode {

  public Node16() {
    keys = new byte[16];
    Arrays.fill(keys, (byte) 0xff);
    ptrs = new ISEARTNode[16];
  }

  public Node16(Node4 n4) {
    this();
    partialKey = n4.partialKey;
    System.arraycopy(n4.keys, 0, keys, 0, 4);
    System.arraycopy(n4.ptrs, 0, ptrs, 0, 4);
  }

  /** Copy and modify from {@linkplain Arrays#binarySearch}. */
  private static int binarySearchUnsignedByteArray(byte[] a, int fromIndex, int toIndex, byte key) {
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

  @Override
  public final int getPtrIdxByByte(byte k) {
    int c = binarySearchUnsignedByteArray(keys, 0, keys.length, k);
    return c >= 0 && ptrs[c] == null ? -c - 1 : c;
  }

  @Override
  public ISEARTNode insert(byte key, int insPos, ISEARTNode child) {
    if (ptrs[15] != null) {
      // to expand since full
      SEARTNode enode = new Node48(this);
      enode.insertOnByteMap(key, child);
      return enode;
    }
    shiftInsert(-insPos - 1, key, child);
    return null;
  }

  /** shift 4 bytes at most. */
  protected final void shiftInsertIn4(int pos, byte kb, ISEARTNode child) {
    if (ubyte(kb) > ubyte(keys[3])) {
      keys[4] = kb;
      ptrs[4] = child;
      return;
    }

    System.arraycopy(keys, pos, keys, pos + 1, 4 - pos);
    System.arraycopy(ptrs, pos, ptrs, pos + 1, 4 - pos);
    keys[pos] = kb;
    ptrs[pos] = child;
  }
}
