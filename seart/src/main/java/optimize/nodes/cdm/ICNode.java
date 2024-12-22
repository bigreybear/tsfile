package optimize.nodes.cdm;

import optimize.merge.MapType;
import optimize.merge.PrefixMergeStrategy;
import optimize.nodes.IMicroNode;
import optimize.util.InfixGroup;

import java.util.function.Function;

public interface ICNode extends IMicroNode {
  // exact type is constrained by context logic
  // void setBranchingKeys(T[] collected);

  // void setBranchingKeys(List<Integer> collect);

  // more than 4 positions
  // void setBranchingKeys(byte[][] bks);

  void setContent(InfixGroup group, Function<byte[], IMicroNode> getLChild, PrefixMergeStrategy mergeStrategy, MapType mapType, int height, boolean EFCoded);

  default int[] getBranchingPos() {
    throw new UnsupportedOperationException();
  }

  // get index of the target key
  default int getBrKeyIdx(int val) {
    throw new UnsupportedOperationException();
  }

  default int getBrKeyIdx(byte[] ba) {
    throw new UnsupportedOperationException();
  }

  // void setBranchingPtr(int idx, ICNode ptr);

  // default void setInterleavedBytes(int idx, byte[] ilb /*Inter-Leaved Bytes*/) {}

  /**
   * Ignore the partial key
   *
   * @param pos assembled by branching and interleaved bytes
   * @return
   */
  default byte[] assembleKeyAt(int pos) {
    return null;
  }

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
}
