package seart;

public class Leaf extends SEARTNode{
  long value;

  public Leaf(byte[] pk, long val) {
    partialKey = pk;
    value = val;
  }

  public Leaf() {

  }

  @Override
  public boolean isLeaf() {
    return true;
  }

  @Override
  public int getPtrIdxByByte(byte k) {
    throw new UnsupportedOperationException("Shall not get pointer from a Leaf.");
  }

  @Override
  public void insertWithExpand(SEARTNode parNode, int parNodeIdx, byte[] insKey, int ofs, int[] res, SEARTNode child) {

  }

  public static void main(String[] args) {
    Leaf a = new Leaf();
    System.out.println(a.value);
  }
}
