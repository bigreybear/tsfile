package optimize.nodes.cdm.frame;

import optimize.nodes.cdm.ICNode;
import optimize.util.InfixGroup;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static optimize.nodes.cdm.ByteEncode.int2Bytes;

public abstract non-sealed class CNode4 extends CNodeBase {

  protected final byte p1, p2, p3, p4;
  protected int[] bks; // indeed a byte[][4] bks; // branching keys

  // exactly no padding on 64-jvm, jdk-17, Compressed OOPs

  // raw keys might with prefix
  public CNode4(int[] pos) {
    if (pos.length > 4)
      throw new UnsupportedOperationException("No more than 4 bytes branching key yet.");

    // pos int init.
    for (int p : pos) {
      if ((p & SINGLE_BYTE_MASK) != 0) throw new RuntimeException("Invalid Branch Pos.");
    }

    p1 = (byte) (pos[0] & 0xff);
    p2 = pos.length > 1 ? (byte) (pos[1] & 0xff) : 0;
    p3 = (pos.length > 2 && p2 != 0) ? (byte) (pos[2] & 0xff) : 0;
    p4 = (pos.length > 3 && p3 != 0) ? (byte) (pos[3] & 0xff) : 0;
  }

  @Override
  public int[] getBranchingPos() {
    // if (p1 == 0) throw new RuntimeException("Invalid branch pos for CNode8");
    if (p2 == 0) return new int[] { p1 & 0xff };
    if (p3 == 0) return new int[] { p1 & 0xff, p2 & 0xff };
    if (p4 == 0) return new int[] { p1 & 0xff, p2 & 0xff, p3 & 0xff };
    return new int[] { p1 & 0xff, p2 & 0xff, p3 & 0xff, p4 & 0xff};
  }

  @Override
  protected int getEmptyKeyIdx() {
    int idx = getKeyPos(0);
    if (idx < 0 || bks[idx] != 0) throw new RuntimeException("Empty key not found.");
    return idx;
  }

  @Override
  protected int getBrKeyIdx(byte[] key, int[] brPos) {
    // todo improve: unnecessary int[] brPos
    // brPos has no trailing zeros.
    int len = brPos.length;
    if (len > 4)
      throw new UnsupportedOperationException("5 or more bytes cannot be encoded to an int.");
    int sk = 0;
    switch (len) {
      case 4:
        sk |= brPos[3] < key.length ? (key[brPos[3]] & 0xFF) : 0;
      case 3:
        sk |= brPos[2] < key.length ? (key[brPos[2]] & 0xFF) << 8 : 0;
      case 2:
        sk |= brPos[1] < key.length ? (key[brPos[1]] & 0xFF) << 16 : 0;
      case 1:
        sk |= (key[brPos[0]] & 0xFF) << 24;
        break;
      default:
        throw new UnsupportedOperationException();
    }
    int idx = getKeyPos(sk);
    if (idx < 0 || bks[idx] != sk) throw new RuntimeException("Key not found.");
    return idx;
  }

  protected abstract int getKeyPos(int k);
}
