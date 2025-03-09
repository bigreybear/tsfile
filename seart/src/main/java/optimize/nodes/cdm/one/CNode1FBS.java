package optimize.nodes.cdm.one;

import optimize.nodes.cdm.ICNode;
import optimize.util.ByteArray;
import optimize.util.InternalInspector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

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
  void initByMap(Map<ByteArray, ICNode> m) {
    keys = new byte[m.size()];
    ptrs = new ICNode[m.size()];
    int i = 0;
    for (Map.Entry<ByteArray, ICNode> entry : m.entrySet()) {
      keys[i] = entry.getKey().getVal()[0];
      ptrs[i] = entry.getValue();
      i++;
    }
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
