package seart;

/**
 * A link to a template. Together with the template root, hold the partial key. Currently, RefNode
 * holds all partial key while template root has no partial key.
 */
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

  @Override
  public boolean isLeaf() {
    return true;
  }

  public static void main(String[] args) {
    System.out.println((byte) 'a');
    System.out.println((byte) 'A');
  }
}
