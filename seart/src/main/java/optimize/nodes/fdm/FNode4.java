package optimize.nodes.fdm;

import java.util.Arrays;
import optimize.util.ArrayHelper;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.info.GraphLayout;

public class FNode4 extends FNodeBase {
  public byte[] keys;
  protected IFNode[] ptrs;

  public FNode4() {
    this.keys = new byte[4];
    this.ptrs = new IFNode[4];
    Arrays.fill(keys, (byte) 0xff);
  }

  @Override
  public void add(byte k, IFNode v) {
    int pos = getPtrIdxByByte(keys, k);
    shiftInsert(-pos - 1, k, keys, v);
  }

  @Override
  public IFNode get(byte k) {
    int idx = getPtrIdxByByte(keys, k);
    if (idx < 0) return null;
    return ptrs[getPtrIdxByByte(keys, k)];
  }

  @Override
  public byte[] getKeysFromFDM() {
    return ArrayHelper.removeTrailing(keys, (byte) 0xff);
  }

  @Override
  public void replace(byte k, IFNode c) {
    for (int i = 0; i < keys.length && ptrs[i] != null; i++) {
      if (keys[i] == k) ptrs[i] = c;
    }
  }

  @Override
  protected IFNode[] getPtrs() {
    return ptrs;
  }

  @Override
  protected String getInspectCode() {
    return "F004";
  }

  public static void main(String[] args) {
    FNode4 n4 = new FNode4();
    n4.setParKey(new byte[] {1});
    System.out.println(ClassLayout.parseInstance(n4.pk).toPrintable());
  }
}
