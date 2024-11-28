package optimize.nodes.fdm.vfull;

import org.openjdk.jol.info.ClassLayout;

import java.nio.charset.StandardCharsets;

import static seart.SEARTNode.ubyte;

public class Node256 implements ISEARTNode {
  byte[] partialKey;
  ISEARTNode[] ptrs;
  byte ptrNum = 0;

  public Node256(int pNum) {
    ptrs = new ISEARTNode[256];
    ptrNum = (byte) pNum;
  }

  public Node256() {
    this(0);
  }

  public Node256(Node48 n48) {
    this();
    partialKey = n48.partialKey;
    for (int i = 0; i < 256; i++) {
      if (n48.keys[i] > 0) {
        ptrs[i] = n48.ptrs[n48.keys[i] - 1];
      }
    }

    ptrNum = 48;
  }

  @Override
  public int getPtrIdxByByte(byte k) {
    return ptrs[ubyte(k)] == null ? -ubyte(k) - 1 : ubyte(k);
  }

  @Override
  public ISEARTNode insert(byte key, int insPos, ISEARTNode child) {
    // todo remove redundant guardian in release ver.
    if (ptrs[ubyte(key)] != null) {
      throw new RuntimeException(
          "Inserting duplicate key:" + (char) key + "," + child);
    }

    ptrs[ubyte(key)] = child;
    ptrNum++;
    return null;
  }

  @Override
  public void insertOnByteMap(byte bk, ISEARTNode child) {
    // todo remove redundant guardian in release ver.
    if (ptrs[ubyte(bk)] != null) {
      throw new RuntimeException("Inserting duplicate key:" + (char) bk + "," + ptrs[ubyte(bk)]);
    }

    ptrs[ubyte(bk)] = child;
    ptrNum++;
  }

  @Override
  public byte[] getKeys() {
    byte[] res = new byte[ubyte(ptrNum)];
    int resNum = 0;
    for (int i = 0; ; i++) {
      if (ptrs[i] != null) {
        res[resNum] = (byte) i;
        resNum++;
      }

      if (resNum == ubyte(ptrNum)) {
        return res;
      }
    }
  }

  @Override
  public byte[] getPartialKey() {
    return partialKey;
  }

  @Override
  public void reassignPartialKey(byte[] pk) {
    partialKey = pk;
  }

  @Override
  public ISEARTNode getChildByPtrIndex(int idx) {
    return ptrs[idx];
  }

  @Override
  public final ISEARTNode getChildByKeyByte(byte b) {
    return ptrs[ubyte(b)];
  }

  @Override
  public void setChildPtrByIndex(int idx, ISEARTNode n) {
    ptrs[idx] = n;
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
    Node256 node256 = new Node256();
    for (int i = 0; i < 256; i++) {
      node256.ptrs[i] = node256;
    }
    System.out.println(ClassLayout.parseInstance(node256).toPrintable());
  }
}
