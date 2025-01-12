package optimize.nodes.cdm;

import optimize.SearchStatus;
import optimize.merge.PrefixMergeStrategy;
import optimize.nodes.IMicroNode;
import optimize.nodes.ITSNode;
import optimize.util.InfixGroup;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static optimize.merge.CDMPrefixMerge.recNextMergeOnCDMV2;
import static optimize.nodes.cdm.ByteEncode.short2Bytes;
import static optimize.nodes.cdm.ByteEncode.short2BytesNoTrailing;
import static optimize.nodes.cdm.CNodeHelper.extractBytes;
import static optimize.util.ArrayHelper.findIntervals;

public class CNode2 extends CNodeBase implements ICNode {
  final byte p1, p2;
  short[] bks;

  public CNode2(int[] pos) {
    if (pos.length > 2)
      throw new UnsupportedOperationException("No more than 2 bytes branching key in CNode2.");

    if (pos[0] == 0 || (pos[0] & SINGLE_BYTE_MASK) != 0
     || (pos.length > 1 && (pos[1] & SINGLE_BYTE_MASK) != 0)) throw new RuntimeException("Invalid Branch Pos.");
    p1 = (byte) pos[0];
    p2 = pos.length > 1 ? (byte) pos[1] : 0;
  }

  // region Essential Interfaces
  // These are essential for build and search.

  @Override
  public int[] getBranchingPos() {
    if (p1 == 0) throw new RuntimeException("Invalid branch pos for CNode2");
    if (p2 == 0) return new int[] {p1};
    return new int[] {p1, p2};
  }

  // a support method for setContent
  @Override
  protected final Function<Integer, List<byte[]>> generateCompleteKeyRetrieval(InfixGroup group) {
    final short[] sbk = group.sortedShortBranchKeys();
    setBranchingKeys(sbk);
    return (integer -> group.getCompleteKeys(short2Bytes(sbk[integer])));
  }

  @Override
  public ICNode getCDMChild(byte[] key, SearchStatus sts) {
    return null;
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

  private void setBranchingKeys(short[] collected) {
    bks = new short[collected.length];
    ptrs = new ICNode[collected.length];
    System.arraycopy(collected, 0, bks, 0, bks.length);
  }

  public static void main(String[] args) {
    CNode2 c2 = new CNode2(new int[] {1,2});
    System.out.println(ClassLayout.parseInstance(c2).toPrintable());
  }

}
