package optimize.nodes.fdm;

import static optimize.nodes.fdm.vfull.SEARTNode.ubyte;
import static optimize.util.ArrayHelper.removeTrailingZeros;

public class FNode256 extends FNodeBase implements IFNode {
  // no prefixed key, but deem b\'00' as prefixed-pointer
  @Override
  public void add(byte k, IFNode v) {
    ptrs[ubyte(k)] = v;
  }

  @Override
  public IFNode get(byte k) {
    return ptrs[ubyte(k)];
  }

  public byte[] getKeysFromFDM() {
    byte[] res = new byte[256];
    for (int i = 0, len = 0; i < 256; i++) {
      if (ptrs[i] != null) res[len++] = (byte) i;
    }
    return removeTrailingZeros(res);
  }

  public FNode256() {
    this.ptrs = new IFNode[256];
  }

  @Override
  public void replace(byte k, IFNode n) {
    ptrs[ubyte(k)] = n;
  }

  @Override
  public long getValue() {
    throw new UnsupportedOperationException();
  }

}
