package seart;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class Node16 extends SEARTNode {

  public Node16() {
    keys = new byte[16];
    Arrays.fill(keys, Byte.MAX_VALUE);
    ptrs = new ISEARTNode[16];
  }

  public Node16(Node4 n4) {
    this();
    partialKey = n4.partialKey;
    System.arraycopy(n4.keys, 0, keys, 0, 4);
    System.arraycopy(n4.ptrs, 0, ptrs, 0, 4);
  }

  @Override
  public final int getPtrIdxByByte(byte k) {
    int c = Arrays.binarySearch(keys, k);
    return c >= 0 && ptrs[c] == null ? -c-1 : c;
  }

  @Override
  public final ISEARTNode insertWithExpand(byte[] insKey, int ofs, int[] res, ISEARTNode child) {
    if (ptrs[15] != null) {
      // to expand since full
      SEARTNode enode = new Node48(this);
      enode.insertOnByteMap(insKey[ofs + res[0]], child);
      return enode;
    }
    shiftInsert(-res[2] - 1, insKey[ofs + res[0]], child);
    return null;
  }

  /**
   * shift 4 bytes at most.
   */
  public final void shiftInsertIn4(int pos, byte kb, ISEARTNode child) {
    System.arraycopy(keys, pos, keys, pos + 1, 4 - pos);
    System.arraycopy(ptrs, pos, ptrs, pos + 1, 4 - pos);
    keys[pos] = kb;
    ptrs[pos] = child;
  }
}
