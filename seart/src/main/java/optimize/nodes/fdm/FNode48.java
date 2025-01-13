package optimize.nodes.fdm;

import java.util.Arrays;

public class FNode48 extends FNodeBase {
  public final byte[] keys = new byte[256];
  protected IFNode[] ptrs;
  private int ptrNum = 0;

  public FNode48() {
    this.ptrs = new IFNode[48];
    Arrays.fill(keys, (byte) -1);
  }

  @Override
  public void add(byte k, IFNode v) {
    keys[ubyte(k)] = (byte) (ptrNum);
    ptrs[ptrNum] = v;
    ptrNum++;
  }

  @Override
  public void replace(byte k, IFNode c) {
    ptrs[keys[ubyte(k)]] = c;
  }

  @Override
  public IFNode get(byte k) {
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

  @Override
  protected IFNode[] getPtrs() {
    return ptrs;
  }

  @Override
  protected String getInspectCode() {
    return "F048";
  }
}
