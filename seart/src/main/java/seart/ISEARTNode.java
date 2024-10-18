package seart;

public interface ISEARTNode {

  // region Common
  byte[] getPartialKey();

  void reassignPartialKey(byte[] pk);

  int[] matchPartialKey(byte[] insKey, int ofs);

  boolean isLeaf();

  default byte[] getKeys() {
    return null;
  }
  // endregion


  ISEARTNode getChildByPtrIndex(int idx);

  ISEARTNode getChildByKeyByte(byte b);

  void setChildPtrByIndex(int idx, ISEARTNode n);

  void shiftInsert(int pos, byte kb, ISEARTNode child);

  /**
   * Consistent with {@link java.util.Arrays#binarySearch}
   *
   * @param k target key value
   * @return result on the {@linkplain SEARTNode#keys}
   */
  int getPtrIdxByByte(byte k);

  /**
   * @param res refer to return of {@linkplain #matchPartialKey};
   * @return expanded node, null if otherwise
   */
  ISEARTNode insertWithExpand(
      byte[] insKey,
      int ofs,
      int[] res,
      ISEARTNode child);

  // todo optimize with virtualization
  void insertOnByteMap(byte bk, ISEARTNode child);

  default long getValue() {
    throw new UnsupportedOperationException();
  }
}
