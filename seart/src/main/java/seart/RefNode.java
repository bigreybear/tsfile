package seart;

public class RefNode implements ISEARTNode {
  public ISEARTNode templateRoot;
  byte[] parKey;
  public long[] values;

  public void setValues(long[] v) {
    values = v;
  }

  public void setTemplateRoot(ISEARTNode tr) {
    templateRoot = tr;
  }

  @Override
  public byte[] getPartialKey() {
    return parKey;
  }

  @Override
  public void reassignPartialKey(final byte[] pk) {
    parKey = pk;
  }

  /**
   * Override MAGIC return array.
   * @return [signal (1 for success), the value from template, ...]
   */
  @Override
  public int[] matchPartialKey(byte[] insKey, int ofs) {
    if (parKey != null) {
      for (int i = 0; i < parKey.length; i++) {
        if (parKey[i] != insKey[ofs + i]) {
          return new int[] {-1, 0, 0};
        }
      }
    }

    // todo remove redundant check
    if (templateRoot.getPartialKey() != null) {
      throw new RuntimeException("Template partial key not cleared.");
    }

    return new int[] {1, (int) SEARTree.search(templateRoot, insKey, ofs + parKey.length), 0};
  }

  @Override
  public boolean isLeaf() {
    return true;
  }

  @Override
  public ISEARTNode getChildByPtrIndex(int idx) {
    return null;
  }

  @Override
  public ISEARTNode getChildByKeyByte(byte b) {
    return null;
  }

  @Override
  public void setChildPtrByIndex(int idx, ISEARTNode n) {

  }

  @Override
  public void shiftInsert(int pos, byte kb, ISEARTNode child) {

  }

  @Override
  public int getPtrIdxByByte(byte k) {
    return 0;
  }

  @Override
  public ISEARTNode insertWithExpand(byte[] insKey, int ofs, int[] res, ISEARTNode child) {
    return null;
  }

  @Override
  public void insertOnByteMap(byte bk, ISEARTNode child) {

  }

  public static void main(String[] args) {
    System.out.println((byte)'a');
    System.out.println((byte)'A');
  }
}
