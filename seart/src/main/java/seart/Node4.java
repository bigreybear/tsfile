package seart;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Node4 extends SEARTNode {

  public Node4() {
    keys = new byte[4];
    Arrays.fill(keys, Byte.MAX_VALUE);
    ptrs = new ISEARTNode[4];
  }

  public Node4(byte[] pk, byte k1, ISEARTNode p1, byte k2, ISEARTNode p2) {
    this();
    partialKey = pk;
    if (k1 < k2) {
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
  public final int getPtrIdxByByte(byte k) {
    for (int i = 0; i < 4; i++) {
      if (k <= keys[i]) {
        if (k == keys[i]) {
          return ptrs[i] == null ? -(i + 1) : i;
        }
        return -(i + 1);
      }
    }
    if (ptrs[3] == null) {
      throw new UnsupportedOperationException("Illegal byte for search:" + k);
    } else {
      return -4;
    }
  }

  @Override
  public final ISEARTNode insertWithExpand(byte[] insKey, int ofs, int[] res, ISEARTNode child) {
    if (ptrs[3] != null) {
      // to expand since full
      Node16 nnode = new Node16(this);
      nnode.shiftInsertIn4(-res[2] - 1, insKey[ofs + res[0]], child);
      return nnode;
    }
    shiftInsert(-res[2] - 1, insKey[ofs + res[0]], child);
    return null;
  }

  public static void main(String[] args) {
    SEARTNode n4 = new Node4();
    n4.keys = new byte[]{'a', 'h', 'z', Byte.MAX_VALUE};
    n4.partialKey = "root.".getBytes(StandardCharsets.UTF_8);
    byte[] ik = "prefix.root.b".getBytes(StandardCharsets.UTF_8);

    int[] res = n4.matchPartialKey(ik, 7);
    System.out.println(Arrays.toString(res));
    n4.insertWithExpand(ik, 7, res, n4);
    System.out.println(n4);
  }
}
