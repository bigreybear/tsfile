package optimize.nodes.cdm;

import optimize.nodes.INode;

import java.util.List;

public interface ICNode extends INode {
  void setBranchingKeys(List<Integer> collect);

  default void setBranchingPtr(int idx, INode ptr) {throw new UnsupportedOperationException();};

  default void setInterleavedBytes(int idx, byte[] ilb /*Inter-Leaved Bytes*/ ) {};

  default byte[] assembleKeyAt(int pos) {return null;};

  void setPartialKey(byte[] b);
}
