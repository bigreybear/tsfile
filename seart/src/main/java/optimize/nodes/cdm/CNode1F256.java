package optimize.nodes.cdm;



import optimize.nodes.NodeInspector;

import java.util.ArrayList;
import java.util.List;


/** Imitate FNode48 */
public class CNode1F256 extends CNodeOneBase {

  @Override
  public List<byte[]> getKeyBytes() {
    List<byte[]> r = new ArrayList<>();
    for (int i = 0; i < ptrs.length; i++) {
      if (ptrs[i] != null) r.add(new byte[] {(byte) i});
    }
    return r;
  }

  @Override
  protected void compactInit(byte[] sbk) {
    ptrs = new ICNode[256];
  }

  @Override
  protected void setPointer(byte[] sbk, int i, ICNode c) {
    ptrs[ubyte(sbk[i])] = c;
  }

  @Override
  protected ICNode getPointer(byte b) {
    if (ptrs[ubyte(b)] == null) throw new RuntimeException("Invalid search Byte.");
    return ptrs[ubyte(b)];
  }

  @Override
  protected String codeName() {
    return "CNode1F256";
  }

  @Override
  public void acceptInspector(NodeInspector noi) {
    super.acceptInspector(noi);
    noi.appendEntry("CNode1F256_key_num", getKeyBytes().size());
  }
}
