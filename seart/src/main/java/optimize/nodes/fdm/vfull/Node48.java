package optimize.nodes.fdm.vfull;

import java.nio.charset.StandardCharsets;

public class Node48 extends SEARTNode {
  byte ptrNum = 0;

  public Node48(int pNum) {
    // note that value of keys are pointer index, different from Node4/16
    keys = new byte[256];
    ptrs = new ISEARTNode[48];
    ptrNum = (byte) pNum;
  }

  public Node48() {
    this(0);
  }

  public Node48(Node16 n16) {
    this();
    partialKey = n16.partialKey;
    for (byte i = 0; i < 16; i++) {
      keys[ubyte(n16.keys[i])] = (byte) (i + 1); // elements of key array are (indices of ptr + 1)
    }
    System.arraycopy(n16.ptrs, 0, ptrs, 0, 16);
    ptrNum = 16;
  }

  @Override
  public final int getPtrIdxByByte(byte k) {
    return keys[ubyte(k)] - 1;
  }

  @Override
  public ISEARTNode insert(byte key, int insPos, ISEARTNode child) {
    if (ptrNum == 48) {
      // to expand since full
      ISEARTNode enode = new Node256(this);
      enode.insertOnByteMap(key, child);
      return enode;
    }
    insertOnByteMap(key, child);
    return null;
  }

  @Override
  public byte[] getKeys() {
    byte[] res = new byte[ptrNum];
    int resNum = 0;
    for (int i = 0; ; i++) {
      if (keys[i] > 0) {
        res[resNum] = (byte) i;
        resNum++;
      }

      if (resNum == ptrNum) {
        return res;
      }
    }
  }

  @Override
  public final void insertOnByteMap(byte bk, ISEARTNode child) {
    // pointers are continuous, makes insert more efficient while delete more slow

    // todo remove this redundant guardian in release ver.
    if (keys[ubyte(bk)] >= 1 && ptrs[keys[ubyte(bk)] - 1] != null) {
      throw new RuntimeException(
          "Inserting duplicate key:" + (char) bk + "," + ptrs[keys[ubyte(bk)] - 1]);
    }

    keys[ubyte(bk)] = (byte) (ptrNum + 1);
    ptrs[ptrNum] = child;
    ptrNum++;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    byte[] ks = getKeys();
    for (int i = 0; i < ks.length; i++) {
      builder.append(String.format("(%s,%d)", (char) ks[i], getPtrIdxByByte(ks[i])));
      if (i != ks.length - 1) {
        builder.append(",");
      }
    }
    return new String(partialKey, StandardCharsets.UTF_8) + ":{" + builder + "}";
  }

  public static void main(String[] args) {
    Node48 node48 = new Node48();
    Leaf leaf = new Leaf();
    for (int i = 0; i < 256; i++) {
      if (i % 9 == 0) {
        node48.testInsert((byte) i, leaf);
      }
    }

    System.out.println("HERE");
  }

  private void testInsert(byte k, ISEARTNode child) {
    keys[ubyte(k)] = ptrNum;
    ptrs[ptrNum] = child;
    ptrNum++;
  }
}
