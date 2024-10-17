package seart;

public class Node48 extends SEARTNode {
  short ptrNum = 0;

  public Node48() {
    // note that value of keys are pointer index, different from Node4/16
    keys = new byte[256];
    ptrs = new SEARTNode[48];
  }

  public Node48(Node16 n16) {
    this();
    partialKey = n16.partialKey;
    for (byte i = 0; i < 16; i++) {
      keys[n16.keys[i]] = i;
    }
    System.arraycopy(n16.ptrs, 0, ptrs, 0, 16);
    ptrNum = 16;

  }

  @Override
  public int getPtrIdxByByte(byte k) {
    return keys[k]-1;
  }

  @Override
  public void insertWithExpand(SEARTNode parNode, int parNodeIdx, byte[] insKey, int ofs, int[] res, SEARTNode child) {

  }
}
