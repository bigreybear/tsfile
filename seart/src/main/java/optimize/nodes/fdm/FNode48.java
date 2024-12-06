package optimize.nodes.fdm;

import static optimize.nodes.fdm.vfull.SEARTNode.ubyte;

import optimize.nodes.INode;

public class FNode48 extends FNode256 {
  public final byte[] keys = new byte[256];
  private int ptrNum = 0;

  public FNode48() {
    this.ptrs = new INode[48];
  }

  @Override
  public void add(byte k, INode v) {
    keys[ubyte(k)] = (byte) (ptrNum);
    ptrs[ptrNum] = v;
    ptrNum++;
  }

  @Override
  public INode get(byte k) {
    return ptrs[keys[ubyte(k)]];
  }
}
