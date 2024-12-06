package optimize.nodes.cdm;

import java.util.List;
import optimize.nodes.INode;

public interface ICNode extends INode {
  void setBranchingKeys(List<Integer> collect);

  // more than 4 positions
  default void setBranchingKeysExtended(byte[][] bks) {}
  ;

  default int[] getBranchingPos() {
    return null;
  }

  // get index of the target key
  default int getBrKeyIdx(int val) {
    return -1;
  }

  default int getBrKeyIdx(byte[] ba) {
    return -1;
  }

  default void setBranchingPtr(int idx, INode ptr) {
    throw new UnsupportedOperationException();
  }
  ;

  default void setInterleavedBytes(int idx, byte[] ilb /*Inter-Leaved Bytes*/) {}
  ;

  /**
   * @param src may have trailing 0s, so could be longer than pos[-1] or res
   * @param pos controlled len of the res
   */
  static byte[] setBytesByPos(byte[] res, byte[] src, int[] pos) {
    if (res == null
        || src == null
        || pos == null
        || src.length < pos.length
        || res.length < pos.length) throw new RuntimeException("Input Error");

    for (int i = 0; i < pos.length; i++) {
      res[pos[i]] = src[i];
    }

    return res;
  }

  static int[] shiftIntArr(int[] b, int shift) {
    int[] res = new int[b.length];
    for (int i = 0; i < b.length; i++) {
      res[i] = b[i] + shift;
    }
    return res;
  }

  static int[] unsignedByteArr2IntArr(byte[] b) {
    int[] intArr = new int[b.length];
    for (int i = 0; i < intArr.length; i++) {
      intArr[i] = 0xff & b[i];
    }
    return intArr;
  }

  /**
   * Ignore the partial key
   *
   * @param pos assembled by branching and interleaved bytes
   * @return
   */
  default byte[] assembleKeyAt(int pos) {
    return null;
  }
  ;

  default ICNode getPtrByPos(int pos) {
    return null;
  }

  void setPartialKey(byte[] b);
}
