package optimize.nodes.cdm;

import optimize.nodes.IMicroNode;
import optimize.nodes.NodeInspector;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Imitate FNode48, uses byte array as indexes of ptr array.
 * <p>Dynamically compacted as {@linkplain #compactInit} indicated. */
public class CNode1F48 extends CNodeOneBase {
  // serves as an index to the ptrs
  byte[] keys;
  byte keyNum = 0;

  @Override
  public List<byte[]> getKeyBytes() {
    List<byte[]> r = new ArrayList<>();
    for (byte k : keys) {
      if (k >= 0) r.add(new byte[] {k});
    }
    return r;
  }

  @Override
  protected void compactInit(byte[] sbk) {
    // compact threshold
    int t = sbk[sbk.length-1] - sbk[0] + 1;
    if(t > 254) {
      keys = new byte[256];
      Arrays.fill(keys, (byte) 0xff);
      ptrs = new ICNode[48];
    } else {
      keys = new byte[t+1];
      Arrays.fill(keys, (byte) 0xff);
      keys[0] = sbk[0]; // working as base
      ptrs = new ICNode[sbk.length];
    }

  }

  @Override
  protected void setPointer(byte[] sbk, int i, ICNode c) {
    if (keys.length == 256) {
      keys[ubyte(sbk[i])] = keyNum;
    } else {
      keys[ubyte(sbk[i]-sbk[0])+1] = keyNum;
    }
    ptrs[keyNum++] = c;
  }

  @Override
  protected ICNode getPointer(byte b) {
    if (keys.length == 256) {
      int i = keys[ubyte(b)];
      if (i<0) throw new RuntimeException("Invalid search Byte.");
      return ptrs[i];
    } else {
      // notice any value in keys is less than 48 so no worry sign
      return ptrs[keys[b-keys[0]+1]];
    }
  }

  @Override
  protected String codeName() {
    return "CNode1F48";
  }

  @Override
  public void acceptInspector(NodeInspector noi) {
    super.acceptInspector(noi);
    noi.appendEntry("CNode1F48_key_num", keyNum);
  }
}
