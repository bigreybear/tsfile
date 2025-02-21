package optimize.nodes.cdm.one;

import optimize.nodes.cdm.ICNode;
import optimize.util.InternalInspector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static optimize.IntegratedMain.INTERNAL_PROFILE;


// BS for binary search
public class CNode1FBS extends CNodeOneBase {
  // ordered as signed-bytes, parallel to the ptrs
  byte[] keys;

  @Override
  public List<byte[]> getKeyBytes() {
    List<byte[]> r = new ArrayList<>(keys.length);
    for (byte k : keys) {
      r.add(new byte[] {k});
    }
    return r;
  }

  @Override
  protected void compactInit(byte[] sbk) {
    keys = Arrays.copyOfRange(sbk, 0, sbk.length);
    ptrs = new ICNode[sbk.length];
  }

  @Override
  protected void setPointer(byte[] sbk, int idx, ICNode c) {
    ptrs[idx] = c;
  }

  @Override
  protected ICNode getPointer(byte b) {
    if (!INTERNAL_PROFILE) {
      return ptrs[Arrays.binarySearch(keys, b)];
    } else {
      long watch = System.nanoTime();
      ICNode ptr = ptrs[Arrays.binarySearch(keys, b)];
      watch = System.nanoTime() - watch;
      InternalInspector.appendEntry(codeName() + "_query_time", watch);
      return ptr;
    }
  }

  @Override
  protected String codeName() {
    return "CNode1FBS";
  }
}
