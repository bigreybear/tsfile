package seart;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public abstract class SEARTNode implements ISEARTNode {
  // shared structure of Node4/16/48
  byte[] partialKey;
  byte[] keys;
  ISEARTNode[] ptrs;

  public static int ubyte(byte b) {
    return b & 0xff;
  }

  @Override
  public byte[] getPartialKey() {
    return partialKey;
  }

  @Override
  public final ISEARTNode getChildByPtrIndex(int idx) {
    // Leaf shall not access this method by control logic
    return ptrs[idx];
  }

  @Override
  public final ISEARTNode getChildByKeyByte(byte b) {
    int c = getPtrIdxByByte(b);
    return c < 0 ? null : ptrs[c];
  }

  @Override
  public final void setChildPtrByIndex(int idx, ISEARTNode n) {
    ptrs[idx] = n;
  }

  @Override
  public final void shiftInsert(int pos, byte kb, ISEARTNode child) {
    System.arraycopy(keys, pos, keys, pos + 1, keys.length - pos - 1);
    System.arraycopy(ptrs, pos, ptrs, pos + 1, ptrs.length - pos - 1);
    keys[pos] = kb;
    ptrs[pos] = child;
  }

  @Override
  public boolean isLeaf() {
    return false;
  }

  @Override
  public byte[] getKeys() {
    int num = 0;
    while (num < ptrs.length && ptrs[num] != null) num++;
    return Arrays.copyOfRange(keys, 0, num);
  }

  // only for initialization
  @Override
  public final void reassignPartialKey(byte[] pk) {
    partialKey = pk;
  }

  @Override
  public String toString() {
    byte[] byteArray = getKeys();
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < byteArray.length; i++) {
      sb.append((char) byteArray[i]);
      if (i < byteArray.length - 1) {
        sb.append(", ");
      }
    }
    return String.format(
        " %s : {%s}",
        partialKey == null ? "(null)" : new String(partialKey, StandardCharsets.UTF_8), sb);
  }
}
