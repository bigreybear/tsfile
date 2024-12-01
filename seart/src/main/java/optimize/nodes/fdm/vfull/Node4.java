package optimize.nodes.fdm.vfull;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class Node4 extends SEARTNode {

  public Node4() {
    keys = new byte[4];
    Arrays.fill(keys, (byte) 0xff);
    ptrs = new ISEARTNode[4];
  }

  public Node4(byte[] pk, byte k1, ISEARTNode p1, byte k2, ISEARTNode p2) {
    this();
    partialKey = pk;
    if (ubyte(k1) < ubyte(k2)) {
      keys[0] = k1;
      keys[1] = k2;
      ptrs[0] = p1;
      ptrs[1] = p2;
    } else {
      keys[0] = k2;
      keys[1] = k1;
      ptrs[0] = p2;
      ptrs[1] = p1;
    }
  }

  @Override
  public SEARTNode getPrefixed(ISEARTNode leaf) {
    Node4Prefixed n4p = new Node4Prefixed();
    System.arraycopy(keys, 0, n4p.keys, 0, keys.length);
    System.arraycopy(ptrs, 0, n4p.ptrs, 0, ptrs.length);
    n4p.partialKey = partialKey;
    n4p.prefixedPtr = leaf;
    return n4p;
  }

  @Override
  public final int getPtrIdxByByte(byte k) {
    int sk = ubyte(k);
    for (int i = 0; i < 4; i++) {
      if (sk == ubyte(keys[i])) {
        return ptrs[i] == null ? -(i + 1) : i;
      }
      if (sk < ubyte(keys[i])) {
        return -(i + 1);
      }
    }
    // the byte is larger than all existed ones
    if (ptrs[3] == null) {
      throw new UnsupportedOperationException(
          "Incurring an erroneous Node4 when searching byte:" + k);
    } else {
      return -4;
    }
  }

  @Override
  public ISEARTNode insert(byte key, int insPos, ISEARTNode child) {
    if (ptrs[3] != null) {
      // to expand since full
      Node16 nnode = new Node16(this);
      nnode.shiftInsertIn4(-insPos - 1, key, child);
      return nnode;
    }
    shiftInsert(-insPos - 1, key, child);
    return null;
  }

  public static void main(String[] args) {
    SEARTNode n4 = new Node4();
    n4.keys = new byte[] {'a', 'h', 'z', Byte.MAX_VALUE};
    n4.partialKey = "root.".getBytes(StandardCharsets.UTF_8);
    byte[] ik = "prefix.root.b".getBytes(StandardCharsets.UTF_8);
  }
}
