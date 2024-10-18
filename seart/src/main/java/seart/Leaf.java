package seart;

import java.nio.charset.StandardCharsets;

public class Leaf implements ISEARTNode {
  byte[] partialKey;
  long value;

  public Leaf(byte[] pk, long val) {
    partialKey = pk;
    value = val;
  }

  public Leaf() {

  }

  @Override
  public final boolean isLeaf() {
    return true;
  }

  @Override
  public final byte[] getPartialKey() {
    return partialKey;
  }

  @Override
  public long getValue() {
    return value;
  }

  public void setValue(long val) {
    value = val;
  }

  @Override
  public final void reassignPartialKey(byte[] pk) {
    partialKey = pk;
  }

  /**
   * Deliberately duplicate {@link SEARTNode#matchPartialKey}.
   */
  @Override
  public final int[] matchPartialKey(byte[] insKey, int ofs) {
    if (partialKey == null || partialKey.length == 0) {
      return new int[]{0, 0, -1};
    }

    int i = 0;
    while (i + ofs < insKey.length
        && i < partialKey.length
        && partialKey[i] == insKey[ofs + i]) {
      i++;
    }

    // pk is exhausted while ins key is not, try to find ptr
    if (i == partialKey.length && i + ofs < insKey.length) {
      return new int[]{i, partialKey.length, -1};
    }

    // pk not exhaust, return matched/overlapped len as in array[0]
    return new int[]{i, partialKey.length, -1};
  }

  @Override
  public String toString() {
    return String.format(" %s : %d", new String(partialKey, StandardCharsets.UTF_8), value);
  }

  // region Unsupported

  @Override
  public void insertOnByteMap(byte bk, ISEARTNode child) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SEARTNode getChildByPtrIndex(int idx) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ISEARTNode getChildByKeyByte(byte b) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setChildPtrByIndex(int idx, ISEARTNode n) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void shiftInsert(int pos, byte kb, ISEARTNode child) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getPtrIdxByByte(byte k) {
    throw new UnsupportedOperationException("Shall not get pointer from a Leaf.");
  }

  @Override
  public ISEARTNode insertWithExpand(byte[] insKey, int ofs, int[] res, ISEARTNode child) {
    throw new UnsupportedOperationException();
  }

  // endregion

  public static void main(String[] args) {
    Leaf a = new Leaf();
    System.out.println(a.value);
  }
}
