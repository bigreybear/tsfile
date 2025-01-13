package optimize.nodes.cdm;

import optimize.nodes.IMicroNode;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Imitate FNode48 */
public class CNode1F256 extends CNodeOneBase {
  ICNode[] ptrs = new ICNode[256];

  @Override
  public List<byte[]> getKeyBytes() {
    List<byte[]> r = new ArrayList<>();
    for (int i = 0; i < ptrs.length; i++) {
      if (ptrs[i] != null) r.add(new byte[] {(byte) i});
    }
    return r;
  }

  @Override
  public List<IMicroNode> getChildren() {
    return Arrays.asList(ptrs);
  }

  @Override
  public int[] getBranchingPos() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void setPointer(byte b, ICNode c) {
    ptrs[ubyte(b)] = c;
  }

  @Override
  protected ICNode getPointer(byte b) {
    if (ptrs[ubyte(b)] == null) throw new RuntimeException("Invalid search Byte.");
    return ptrs[ubyte(b)];
  }
}
