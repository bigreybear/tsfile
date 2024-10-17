package seart;

import java.util.Arrays;

public class Node16 extends SEARTNode {

  public Node16() {
    keys = new byte[16];
    Arrays.fill(keys, Byte.MAX_VALUE);
    ptrs = new SEARTNode[16];
  }

  public Node16(Node4 n4) {
    partialKey = n4.partialKey;
    System.arraycopy(n4.keys, 0, keys, 0, 4);
    System.arraycopy(n4.ptrs, 0, ptrs, 0, 4);
  }

  @Override
  public int getPtrIdxByByte(byte k) {
    return Arrays.binarySearch(keys, k);
  }

  @Override
  public void insertWithExpand(SEARTNode parNode, int parNodeIdx, byte[] insKey, int ofs, int[] res, SEARTNode child) {
    if (ptrs[15] != null) {
      // to expand since full
      SEARTNode enode = new Node48(this);
      parNode.setChildPtrByIndex(parNodeIdx, enode);

      return;
    }
    shiftInsert(-res[2]-1, insKey[ofs+res[0]], child);
  }
}
