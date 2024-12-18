package optimize.nodes.fdm;

import static optimize.nodes.fdm.vfull.SEARTNode.ubyte;

import java.util.Arrays;
import optimize.nodes.INode;

public class FNode48 extends FNode256 {
  public final byte[] keys = new byte[256];
  private int ptrNum = 0;

  public FNode48() {
    this.ptrs = new INode[48];
    Arrays.fill(keys, (byte) -1);
  }

  @Override
  public void add(byte k, INode v) {
    keys[ubyte(k)] = (byte) (ptrNum);
    ptrs[ptrNum] = v;
    ptrNum++;
  }

  @Override
  public void replace(byte k, INode c) {
    ptrs[keys[ubyte(k)]] = c;
  }

  @Override
  public INode replace(byte[] k, INode c) {
    ptrs[keys[ubyte(k[0])]] = c;
    return this;
  }

  @Override
  public INode get(byte k) {
    if (keys[ubyte(k)] < 0) return null;
    return ptrs[keys[ubyte(k)]];
  }

  @Override
  public byte[] getKeysFromFDM() {
    byte[] res = new byte[ptrNum];
    for (int i = 0, curLen = 0; i < keys.length; i++) {
      if (keys[i] >= 0) res[curLen++] = (byte) i;
      if (curLen == ptrNum) break;
    }
    return res;
  }
}
