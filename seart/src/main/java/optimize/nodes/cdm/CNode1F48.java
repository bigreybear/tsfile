package optimize.nodes.cdm;

import optimize.nodes.IMicroNode;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Imitate FNode48 */
public class CNode1F48 extends CNodeOneBase {
  final byte[] keys = new byte[256];
  final ICNode[] ptrs = new ICNode[48];
  byte keyNum = 0;

  public CNode1F48() {
    Arrays.fill(keys, (byte) 0xff);
  }

  @Override
  public List<byte[]> getKeyBytes() {
    List<byte[]> r = new ArrayList<>();
    for (byte k : keys) {
      if (k >= 0) r.add(new byte[] {k});
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
    keys[ubyte(b)] = keyNum;
    ptrs[keyNum++] = c;
  }

  @Override
  protected ICNode getPointer(byte b) {
    int i = keys[ubyte(b)];
    if (i<0) throw new RuntimeException("Invalid search Byte.");
    return ptrs[i];
  }
}
