package seart;

public abstract class SEARTNode {
  byte[] partialKey;
  byte[] keys;
  SEARTNode[] ptrs;

  /**
   * MAGIC array for efficiency.
   * @return [length of the overlapped bytes, pk.len, index of pointer array]
   */
  public int[] matchPartialKey(byte[] insKey, final int ofs) {
    if (partialKey == null || partialKey.length == 0) {
      return new int[] {0, 0, getPtrIdxByByte(insKey[ofs])};
    }

    int i = 0;
    while (i + ofs < insKey.length
              && i < partialKey.length
              && partialKey[i] == insKey[ofs + i]) {
      i++;
    }

    // pk is exhausted while ins key is not, try to find ptr
    if (i == partialKey.length && i + ofs < insKey.length) {
      return new int[] {i, partialKey.length, getPtrIdxByByte(insKey[i+ofs])};
    }

    // pk not exhaust, return matched/overlapped len as in array[0]
    return new int[] {i, partialKey.length, -1};
  }

  public final SEARTNode getChildByPtrIndex(int idx) {
    // Leaf shall not access this method by control logic
    return ptrs[idx];
  }

  public final void setChildPtrByIndex(int idx, SEARTNode n) {
    ptrs[idx] = n;
  }

  public final void shiftInsert(int pos, byte kb, SEARTNode child) {
    System.arraycopy(keys, pos, keys, pos+1, keys.length-pos-1);
    System.arraycopy(ptrs, pos, ptrs, pos+1, ptrs.length-pos-1);
    keys[pos] = kb;
    ptrs[pos] = child;
  }

  /**
   * Consistent with {@link java.util.Arrays#binarySearch}
   * @param k target key value
   * @return result on the {@linkplain #keys}
   */
  public abstract int getPtrIdxByByte(byte k);

  /**
   *
   * @param res refer to return of {@linkplain #matchPartialKey}
   */
  public abstract void insertWithExpand(
      SEARTNode parNode,
      int parNodeIdx,
      byte[] insKey,
      int ofs,
      int[] res,
      SEARTNode child);

  public boolean isLeaf() {
    return false;
  }

  // only for initialization
  public void reassignPartialKey(byte[] pk) {
    partialKey = new byte[pk.length];
    System.arraycopy(pk, 0, partialKey, 0, partialKey.length);
  }
}
