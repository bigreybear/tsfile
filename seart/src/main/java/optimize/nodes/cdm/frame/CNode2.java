package optimize.nodes.cdm.frame;

import optimize.nodes.cdm.ICNode;
import optimize.util.InfixGroup;

import java.util.List;
import java.util.function.Function;

import static optimize.nodes.cdm.ByteEncode.short2Bytes;

public abstract non-sealed class CNode2 extends CNodeBase {

  final byte p1, p2;
  protected short[] bks;

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

  @Override
  public int[] getBranchingPos() {
    if (p1 == 0) throw new RuntimeException("Invalid branch pos for CNode2");
    if (p2 == 0) return new int[] {p1 & 0xff};
    return new int[] {p1 & 0xff, p2 & 0xff};
  }

  // supporters for proceedQuery
  @Override
  protected int getEmptyKeyIdx() {
    int idx = getKeyPos((short) 0);
    if (idx < 0 || bks[idx] != 0) throw new RuntimeException("Empty key not found.");
    return idx;
  }

  @Override
  protected int getBrKeyIdx(byte[] key, int[] brPos) {
    short s = 0;
    s |= (short) (key[brPos[0]] << 8);
    if (brPos.length > 1 && brPos[1] < key.length) s |= (short) (key[brPos[1]] & 0xff);

    int idx = getKeyPos(s);
    if (idx < 0 || bks[idx] != s) throw new RuntimeException("Key not found.");
    return idx;
  }

  protected abstract int getKeyPos(short k);
}
