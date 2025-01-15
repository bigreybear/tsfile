package optimize.nodes.cdm;

import static optimize.nodes.cdm.ByteEncode.short2Bytes;
import static optimize.nodes.cdm.ByteEncode.short2BytesNoTrailing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import optimize.nodes.NodeInspector;
import optimize.util.InfixGroup;
import org.openjdk.jol.info.ClassLayout;

public class CNode2 extends CNodeBase {
  final byte p1, p2;
  short[] bks;

  public CNode2(int[] pos) {
    if (pos.length > 2)
      throw new UnsupportedOperationException("No more than 2 bytes branching key in CNode2.");

    if (pos[0] == 0
        || (pos[0] & SINGLE_BYTE_MASK) != 0
        || (pos.length > 1 && (pos[1] & SINGLE_BYTE_MASK) != 0))
      throw new RuntimeException("Invalid Branch Pos.");
    p1 = (byte) pos[0];
    p2 = pos.length > 1 ? (byte) pos[1] : 0;
  }

  // region Essential Interfaces
  // These are essential for build and search.

  @Override
  public int[] getBranchingPos() {
    if (p1 == 0) throw new RuntimeException("Invalid branch pos for CNode2");
    if (p2 == 0) return new int[] {p1 & 0xff};
    return new int[] {p1 & 0xff, p2 & 0xff};
  }

  // a support method for setContent
  @Override
  protected final Function<Integer, List<byte[]>> generateCompleteKeyRetrieval(InfixGroup group) {
    final short[] sbk = group.sortedShortBranchKeys();
    bks = new short[sbk.length];
    ptrs = new ICNode[sbk.length];
    System.arraycopy(sbk, 0, bks, 0, bks.length);
    return (integer -> group.getCompleteKeys(short2Bytes(sbk[integer])));
  }

  // supporters for proceedQuery
  @Override
  protected int getEmptyKeyIdx() {
    int idx = Arrays.binarySearch(bks, (short) 0);
    if (idx < 0 || bks[idx] != 0) throw new RuntimeException("Empty key not found.");
    return idx;
  }

  @Override
  protected int getBrKeyIdx(byte[] key, int[] brPos) {
    short s = 0;
    s |= (short) (key[brPos[0]] << 8);
    if (brPos.length > 1 && brPos[1] < key.length) s |= (short) (key[brPos[1]] & 0xff);

    int idx = Arrays.binarySearch(bks, s);
    if (idx < 0 || bks[idx] != s) throw new RuntimeException("Key not found.");
    return idx;
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
    CNode2 c2 = new CNode2(new int[] {1, 2});
    System.out.println(ClassLayout.parseInstance(c2).toPrintable());
  }

  @Override
  protected String codeName() {
    return "CNode2";
  }

  @Override
  public void acceptInspector(NodeInspector noi) {
    super.acceptInspector(noi);
    noi.incEntry("CNode2_cnt", 1);
  }
}
