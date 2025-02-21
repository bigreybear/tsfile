package optimize.nodes.cdm.sorted;

import static optimize.nodes.cdm.ByteEncode.short2Bytes;
import static optimize.nodes.cdm.ByteEncode.short2BytesNoTrailing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import optimize.IntegratedMain;
import optimize.annotation.DebugOnly;
import optimize.nodes.cdm.ICNode;
import optimize.nodes.cdm.frame.CNode2;
import optimize.util.InfixGroup;
import optimize.util.InternalInspector;
import org.openjdk.jol.info.ClassLayout;

public class SCNode2 extends CNode2 {
  @Override
  protected int getKeyPos(short k) {
    if (IntegratedMain.INTERNAL_PROFILE) {
      @DebugOnly
      long watch = System.nanoTime();
      int res = Arrays.binarySearch(bks, k);
      watch = System.nanoTime() - watch;
      InternalInspector.appendEntry( codeName() + "_query_time", watch);
      return res;
    } else {
      return Arrays.binarySearch(bks, k);
    }
  }

  public SCNode2(int[] pos) {
    super(pos);
  }

  // region Essential Interfaces
  @Override
  protected Object[] setBrKeysAndGenSupFunctions(InfixGroup group) {
    final short[] sbk = group.sortedShortBranchKeys();
    bks = new short[sbk.length];
    ptrs = new ICNode[sbk.length];
    System.arraycopy(sbk, 0, bks, 0, bks.length);
    Object[] res = new Object[2];
    res[0] = (Function<Integer, List<byte[]>>) (integer -> group.getCompleteKeys(short2Bytes(sbk[integer])));
    res[1] = (Function<Integer, Integer>) (i -> i);
    return res;
  }

  @Override
  public List<byte[]> getKeyBytes() {
    List<byte[]> r = new ArrayList<>();
    for (short s : bks) {
      r.add(short2BytesNoTrailing(s));
    }
    return r;
  }

  @Override
  protected byte[] getBrKeyAt(int channel) {
    return short2BytesNoTrailing(bks[channel]);
  }

  // endregion

  public static void main(String[] args) {
    SCNode2 c2 = new SCNode2(new int[] {1, 2});
    System.out.println(ClassLayout.parseInstance(c2).toPrintable());
  }

  @Override
  protected String codeName() {
    return "SCNode2";
  }
}
