package optimize.nodes.cdm;

import java.util.function.Function;
import optimize.SearchStatus;
import optimize.merge.MapType;
import optimize.merge.PrefixMergeStrategy;
import optimize.merge.skeleton.MiniTreeRep;
import optimize.nodes.IMicroNode;
import optimize.nodes.ITSNode;
import optimize.util.InfixGroup;

public interface ICNode extends IMicroNode {

  // for tree consists of MiniTreeReps
  void fillContent(MiniTreeRep rep);

  int[] getBranchingPos(); // no trailing 0s.

  void setContent(
      InfixGroup group,
      Function<byte[], IMicroNode> getLChild,
      MapType mapType,
      PrefixMergeStrategy mergeStrategy,
      int height);

  /**
   * Carry the search progress forward.
   *
   * @param sts the context of the search progress
   * @return
   */
  ICNode proceedQueryCDM(final byte[] key, final SearchStatus sts);

  // currently only used for templates
  default byte[][] getBranchingKeys() {
    throw new UnsupportedOperationException();
  }

  /**
   * Ignore the partial key
   *
   * @param pos assembled by branching and interleaved bytes
   * @return
   */
  default byte[] assembleKeyAt(int pos) {
    throw new UnsupportedOperationException();
  }

  /**
   * @param src may have trailing 0s, so could be longer than pos[-1] or res
   * @param pos controlled len of the res
   */
  static void setBytesByPos(byte[] res, byte[] src, int[] pos) {
    if (res == null
        || src == null
        || pos == null
        || src.length < pos.length
        || res.length < pos.length) throw new RuntimeException("Input Error");

    for (int i = 0; i < pos.length; i++) {
      res[pos[i]] = src[i];
    }
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

  @Override
  default void setChild(byte[] k, IMicroNode n) {
    throw new UnsupportedOperationException();
  }

  @Override
  default void replace(byte[] key, IMicroNode node) {
    throw new UnsupportedOperationException();
  }

  @Override
  default IMicroNode getChild(byte[] key) {
    throw new UnsupportedOperationException();
  }

  @Override
  default ITSNode getLogicalChild(String pathSeg) {
    throw new UnsupportedOperationException();
  }

  @Override
  default long getValue() {
    // low-frequency called so trivial to perf.
    // most descendants do not need this.
    throw new UnsupportedOperationException();
  }
}
