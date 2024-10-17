package seart;

public class Node48 extends SEARTNode {

  public Node48() {
    // note that value of keys are pointer index, different from Node4/16
    keys = new byte[256];
    ptrs = new SEARTNode[48];
  }

  @Override
  public int getPtrIdxByByte(byte k) {
    return keys[k]-1;
  }

  @Override
  public void insertWithExpand(SEARTNode parNode, int parNodeIdx, byte[] insKey, int ofs, int[] res, SEARTNode child) {

  }
}
