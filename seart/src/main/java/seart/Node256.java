package seart;

public class Node256 extends SEARTNode {
  public Node256() {
    ptrs = new SEARTNode[256];
  }

  @Override
  public int getPtrIdxByByte(byte k) {
    return k;
  }

  @Override
  public void insertWithExpand(SEARTNode parNode, int parNodeIdx, byte[] insKey, int ofs, int[] res, SEARTNode child) {

  }
}
