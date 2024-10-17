package seart;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Node4 extends SEARTNode {

  public Node4() {
    keys = new byte[4];
    Arrays.fill(keys, Byte.MAX_VALUE);
    ptrs = new SEARTNode[4];
  }

  @Override
  public int getPtrIdxByByte(byte k) {
    for (int i = 0; i < 4; i++) {
      if (k <= keys[i]) {
        if (k == keys[i]) {
          return ptrs[i] == null ? -(i + 1) : i;
        }
        return -(i + 1);
      }
    }
    throw new UnsupportedOperationException("Illegal byte for search:" + k);
  }

  @Override
  public void insertWithExpand(SEARTNode parNode, int parNodeIdx, byte[] insKey, int ofs, int[] res, SEARTNode child) {
    if (ptrs[3] != null) {
      // to expand since full
      SEARTNode nnode = new Node16();
      parNode.setChildPtrByIndex(parNodeIdx, nnode);
      return;
    }
    shiftInsert(-res[2]-1, insKey[ofs+res[0]], child);
  }

  @Override
  public String toString() {
    List<String> out = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      out.add("<" + ((char)keys[i]) + ","
          + (ptrs[i] == null ? "null" : Integer.toHexString(System.identityHashCode(ptrs[i]))) + ">");
    }
    return Integer.toHexString(System.identityHashCode(this)) + ":{" + String.join(", ", out) + "}";
  }

  public static void main(String[] args) {
    SEARTNode n4 = new Node4();
    n4.keys = new byte[]{'a', 'b', 'z', Byte.MAX_VALUE};
    n4.partialKey = "root.".getBytes(StandardCharsets.UTF_8);
    byte[] ik = "prefix.root.g".getBytes(StandardCharsets.UTF_8);

    int[] res  = n4.matchPartialKey(ik, 7);
    System.out.println(Arrays.toString(res));
    n4.insertWithExpand(n4, 0, ik, 7, res, n4);
    System.out.println(n4);
  }
}
