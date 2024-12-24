package optimize.nodes.cdm;

import optimize.SearchStatus;
import optimize.merge.MapType;
import optimize.merge.PrefixMergeStrategy;
import optimize.nodes.IMicroNode;
import optimize.util.InfixGroup;

import java.util.function.Function;

public interface ICNode extends IMicroNode {

  int[] getBranchingPos();

  void setContent(InfixGroup group,
                  Function<byte[], IMicroNode> getLChild,
                  PrefixMergeStrategy mergeStrategy,
                  MapType mapType,
                  int height,
                  boolean EFCoded);

  ICNode getPtr(int pos);

  /**
   * Carry the search progress forward.
   * @param sts the context of the search progress
   * @return the target ptr w.r.t. the current progress, or itself if the leaf
   */
  ICNode getCDMChild(byte[] key, SearchStatus sts);

  @Deprecated // todo remove it
  default int getBrKeyIdx(int v) {throw new UnsupportedOperationException();}

  @Deprecated // todo remove it
  default int getBrKeyIdx(byte[] v) {throw new UnsupportedOperationException();}

  // get index of the target key
  // default int getBrKeyIdx(int val) {
  //   throw new UnsupportedOperationException();
  // }
  //
  // default int getBrKeyIdx(byte[] ba) {
  //   throw new UnsupportedOperationException();
  // }

  /**
   * Ignore the partial key
   *
   * @param pos assembled by branching and interleaved bytes
   * @return
   */
  default byte[] assembleKeyAt(int pos) {
    return null;
  }

  // @Deprecated methods
  // void setBranchingPtr(int idx, ICNode ptr);
  // default void setInterleavedBytes(int idx, byte[] ilb /*Inter-Leaved Bytes*/) {}
  // exact type is constrained by context logic
  // void setBranchingKeys(T[] collected);
  // void setBranchingKeys(List<Integer> collect);
  // more than 4 positions
  // void setBranchingKeys(byte[][] bks);

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
