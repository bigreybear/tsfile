package seart;

import java.io.Serializable;

public interface ISEARTNode extends Serializable {

  // region Common
  byte[] getPartialKey();

  void reassignPartialKey(byte[] pk);

  default boolean isLeaf() {
    return false;
  }

  default byte[] getKeys() {
    return null;
  }
  // endregion

  default ISEARTNode getChildByPtrIndex(int idx) {
    throw new UnsupportedOperationException();
  }

  default ISEARTNode getChildByKeyByte(byte b) {
    throw new UnsupportedOperationException();
  }

  default void setChildPtrByIndex(int idx, ISEARTNode n) {
    throw new UnsupportedOperationException();
  }

  default void shiftInsert(int pos, byte kb, ISEARTNode child) {
    throw new UnsupportedOperationException();
  }

  /**
   * Consistent with {@link java.util.Arrays#binarySearch}
   *
   * @param k target key value
   * @return result on the {@linkplain SEARTNode#keys}
   */
  default int getPtrIdxByByte(byte k) {
    throw new UnsupportedOperationException();
  }

  default ISEARTNode insert(byte key, int insPos, ISEARTNode child) {throw new UnsupportedOperationException();}

  // easy for debug
  default ISEARTNode insert(byte key, ISEARTNode child) {
    int ip = getPtrIdxByByte(key);
    if (ip >= 0) {
      throw new UnsupportedOperationException("Cannot insert duplicate byte.");
    }
    return insert(key, -ip-1, child);
  }

  // todo optimize with virtualization
  default void insertOnByteMap(byte bk, ISEARTNode child) {};

  default long getValue() {
    throw new UnsupportedOperationException();
  }

  // return the length overlapped by two array
  static int getMatchLength(byte[] pk, byte[] ik, final int ofs) {
    if (pk == null || pk.length == 0) {
      return 0;
    }

    int i = 0;
    int minLength = Math.min(ik.length - ofs, pk.length);
    while (i < minLength && pk[i] == ik[ofs + i]) {
      i++;
    }
    return i;
  }

  static ISEARTNode constructNode(byte[] pk, byte[] keys, ISEARTNode[] child) {
    int siz = keys.length;
    if (siz <= 4) {
      Node4 n4 = new Node4();
      n4.reassignPartialKey(pk);
      System.arraycopy(keys, 0, n4.keys, 0, siz);
      System.arraycopy(child, 0, n4.ptrs, 0, siz);
      return n4;
    } else if (siz <= 16) {
      Node16 n16 = new Node16();
      n16.reassignPartialKey(pk);
      System.arraycopy(keys, 0, n16.keys, 0, siz);
      System.arraycopy(child, 0, n16.ptrs, 0, siz);
      return n16;
    } else if (siz <= 48) {
      Node48 n48 = new Node48(siz);
      n48.reassignPartialKey(pk);
      System.arraycopy(child, 0, n48.ptrs, 0, siz);
      for (int i = 0; i < siz; i++) {
        n48.keys[keys[i] & 0xff] = (byte) (i + 1);
      }

      return n48;
    } else {
      /* Node256 */
      Node256 node256 = new Node256(siz);
      node256.reassignPartialKey(pk);
      for (int i = 0; i < siz; i++) {
        node256.ptrs[keys[i] & 0xff] = child[i];
      }
      return node256;
    }
  }
}
