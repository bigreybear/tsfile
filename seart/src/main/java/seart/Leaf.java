package seart;

import java.nio.charset.StandardCharsets;

public class Leaf implements ISEARTNode {
  byte[] partialKey;
  long value;

  public Leaf(byte[] pk, long val) {
    partialKey = pk;
    value = val;
  }

  public Leaf() {}

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

  @Override
  public String toString() {
    return String.format(" %s : %d", new String(partialKey, StandardCharsets.UTF_8), value);
  }

  // endregion

  public static void main(String[] args) {
    Leaf a = new Leaf();
    System.out.println(a.value);
  }
}
