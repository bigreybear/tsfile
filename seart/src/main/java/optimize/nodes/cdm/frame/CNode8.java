package optimize.nodes.cdm.frame;

import optimize.nodes.cdm.ICNode;
import optimize.util.InfixGroup;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static optimize.nodes.cdm.ByteEncode.int2Bytes;
import static optimize.nodes.cdm.ByteEncode.long2Bytes;

public abstract non-sealed class CNode8 extends CNodeBase {
  protected final byte p1, p2, p3, p4, p5, p6, p7, p8;
  protected long[] bks;

  public CNode8(int[] pos) {
    if (pos.length > 8)
      throw new UnsupportedOperationException("No more than 8 bytes branching key in CNode8.");
    int len = pos.length;

    for (int p : pos) {
      if ((p & SINGLE_BYTE_MASK) != 0) throw new RuntimeException("Invalid Branch Pos.");
    }

    // once a position is 0 (except the first), its followers are all zeros.
    p1 = (byte) (pos[0] & 0xff);
    p2 = len > 1 ? (byte) (pos[1] & 0xff) : 0;
    p3 = (len > 2 && p2 != 0) ? (byte) (pos[2] & 0xff) : 0;
    p4 = (len > 3 && p3 != 0) ? (byte) (pos[3] & 0xff) : 0;
    p5 = (len > 4 && p4 != 0) ? (byte) (pos[4] & 0xff) : 0;
    p6 = (len > 5 && p5 != 0) ? (byte) (pos[5] & 0xff) : 0;
    p7 = (len > 6 && p6 != 0) ? (byte) (pos[6] & 0xff) : 0;
    p8 = (len > 7 && p7 != 0) ? (byte) (pos[7] & 0xff) : 0;
  }

  @Override
  public int[] getBranchingPos() {
    if (p1 == 0) throw new RuntimeException("Invalid branch pos for CNode8");
    if (p2 == 0) return new int[] { p1 & 0xff };
    if (p3 == 0) return new int[] { p1 & 0xff, p2 & 0xff };
    if (p4 == 0) return new int[] { p1 & 0xff, p2 & 0xff, p3 & 0xff };
    if (p5 == 0) return new int[] { p1 & 0xff, p2 & 0xff, p3 & 0xff, p4 & 0xff };
    if (p6 == 0) return new int[] { p1 & 0xff, p2 & 0xff, p3 & 0xff, p4 & 0xff, p5 & 0xff };
    if (p7 == 0) return new int[] { p1 & 0xff, p2 & 0xff, p3 & 0xff, p4 & 0xff, p5 & 0xff, p6 & 0xff };
    if (p8 == 0) return new int[] { p1 & 0xff, p2 & 0xff, p3 & 0xff, p4 & 0xff, p5 & 0xff, p6 & 0xff, p7 & 0xff };
    return new int[] { p1 & 0xff, p2 & 0xff, p3 & 0xff, p4 & 0xff, p5 & 0xff, p6 & 0xff, p7 & 0xff, p8 & 0xff };
  }

  // supporters for proceedQuery
  @Override
  protected int getEmptyKeyIdx() {
    int idx = getKeyPos(0);
    if (idx < 0 || bks[idx] != 0) throw new RuntimeException("Empty key not found.");
    return idx;
  }

  @Override
  protected int getBrKeyIdx(byte[] key, int[] brPos) {
    long l = 0;
    switch (brPos.length) {
      case 8:
        l |= ((long) (brPos[7] < key.length ? key[brPos[7]] : 0) & 0xff);
      case 7:
        l |= ((long) (brPos[6] < key.length ? key[brPos[6]] : 0) & 0xff) << 8;
      case 6:
        l |= ((long) (brPos[5] < key.length ? key[brPos[5]] : 0) & 0xff) << 16;
      case 5:
        l |= ((long) (brPos[4] < key.length ? key[brPos[4]] : 0) & 0xff) << 24;
      case 4:
        l |= ((long) (brPos[3] < key.length ? key[brPos[3]] : 0) & 0xff) << 32;
      case 3:
        l |= ((long) (brPos[2] < key.length ? key[brPos[2]] : 0) & 0xff) << 40;
      case 2:
        l |= ((long) (brPos[1] < key.length ? key[brPos[1]] : 0) & 0xff) << 48;
      case 1:
        l |= (long) (key[brPos[0]] & 0xff) << 56;
        break;
      default:
        throw new RuntimeException("Invalid Branch Position length.");
    }

    int idx = getKeyPos(l);
    if (idx < 0 || bks[idx] != l) throw new RuntimeException("Key not found.");
    return idx;
  }

  protected abstract int getKeyPos(long k);
}
