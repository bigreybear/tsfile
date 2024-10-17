package seart;

import java.util.Arrays;

public class Node16 extends SEARTNode {

  public Node16() {
    keys = new byte[16];
    Arrays.fill(keys, Byte.MAX_VALUE);
    ptrs = new SEARTNode[16];
  }

  @Override
  public int getPtrIdxByByte(byte k) {
    return Arrays.binarySearch(keys, k);
  }

  @Override
  public void insertWithExpand(SEARTNode parNode, int parNodeIdx, byte[] insKey, int ofs, int[] res, SEARTNode child) {

  }
}
