package seart;

import java.nio.charset.StandardCharsets;

public class Node256 extends SEARTNode {
  byte ptrNum = 0;

  public Node256() {
    ptrs = new ISEARTNode[256];
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
    return ptrs[k] == null ? -k-1 : k;
  }

  @Override
  public ISEARTNode insertWithExpand(byte[] insKey, int ofs, int[] res, ISEARTNode child) {
    // todo remove redundant guardian in release ver.
    if (ptrs[insKey[ofs + res[0]]] != null) {
      throw new RuntimeException("Inserting duplicate key:" + (char) insKey[ofs + res[0]] + "," + child);
    }

    ptrs[insKey[ofs + res[0]]] = child;
    ptrNum++;
    return null;
  }

  @Override
  public void insertOnByteMap(byte bk, ISEARTNode child) {
    // todo remove redundant guardian in release ver.
    if (ptrs[bk] != null) {
      throw new RuntimeException("Inserting duplicate key:" + (char) bk + "," + ptrs[bk]);
    }

    ptrs[bk] = child;
    ptrNum++;
  }

  @Override
  public byte[] getKeys() {
    byte[] res = new byte[ptrNum];
    int resNum = 0;
    for (int i = 0; ; i++) {
      if (ptrs[i] != null) {
        res[resNum] = (byte) i;
        resNum++;
      }

      if (resNum == ptrNum) {
        return res;
      }
    }
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
}
