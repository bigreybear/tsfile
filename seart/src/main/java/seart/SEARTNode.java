package seart;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public abstract class SEARTNode implements ISEARTNode {
  byte[] partialKey;
  byte[] keys;
  ISEARTNode[] ptrs;

  @Override
  public byte[] getPartialKey() {
    return partialKey;
  }

  /**
   * MAGIC array for efficiency.
   *
   * @return [length of the overlapped bytes, pk.len, index of pointer array]
   */
  @Override
  public final int[] matchPartialKey(byte[] insKey, final int ofs) {
    if (partialKey == null || partialKey.length == 0) {
      return new int[]{0, 0, getPtrIdxByByte(insKey[ofs])};
    }

    int i = 0;
    while (i + ofs < insKey.length
        && i < partialKey.length
        && partialKey[i] == insKey[ofs + i]) {
      i++;
    }

    // pk is exhausted while ins key is not, try to find ptr
    if (i == partialKey.length && i + ofs < insKey.length) {
      return new int[]{i, partialKey.length, getPtrIdxByByte(insKey[i + ofs])};
    }

    // pk not exhaust, return matched/overlapped len as in array[0]
    return new int[]{i, partialKey.length, -1};
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

  // todo optimize with virtualization
  @Override
  public void insertOnByteMap(byte bk, ISEARTNode child) {
    throw new UnsupportedOperationException();
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
        partialKey == null ? "(null)" : new String(partialKey, StandardCharsets.UTF_8),
        sb);
  }
}
