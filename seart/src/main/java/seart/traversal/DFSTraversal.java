package seart.traversal;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import seart.ISEARTNode;
import seart.RefNode;
import seart.SEARTree;

public class DFSTraversal implements Iterator<ISEARTNode> {
  ISEARTNode root;

  Deque<byte[]> traceBytes = new ArrayDeque<>();
  int tbl = 0; // Trace Byte Length
  Deque<KeyedNode> stack = new ArrayDeque<>();

  public DFSTraversal(ISEARTNode node) {
    root = node;
    byte[] ks = root.getKeys();
    for (int i = ks.length - 1; i >= 0; i--) {
      stack.addLast(new KeyedNode(ks[i], root.getChildByKeyByte(ks[i])));
    }
    if (root.getPartialKey() != null) {
      traceBytes.addLast(root.getPartialKey());
      tbl = root.getPartialKey().length;
    }
  }

  public void reset() {
    traceBytes.clear();
    tbl = 0;
    stack.clear();
    byte[] ks = root.getKeys();
    for (int i = ks.length - 1; i >= 0; i--) {
      stack.addLast(new KeyedNode(ks[i], root.getChildByKeyByte(ks[i])));
    }
    if (root.getPartialKey() != null) {
      traceBytes.addLast(root.getPartialKey());
      tbl = root.getPartialKey().length;
    }
  }

  public static void printAllPathsInStatic(SEARTree tree) {
    printAllPathsInStatic(tree.root);
  }

  public static void printAllPathsInStatic(ISEARTNode root) {
    for (String s : getAllPaths(root)) {
      System.out.println(s);
    }
  }

  public void printAllPaths() {
    Map<Integer, List<byte[]>> templatePathBytes = new HashMap<>();
    ISEARTNode node;
    while (hasNext()) {
      node = next();
      if (node.isLeaf()) {
        if (node instanceof RefNode) {
          ISEARTNode t1 = ((RefNode) node).templateRoot;
          List<byte[]> pb = templatePathBytes.getOrDefault(t1.hashCode(), null);
          if (pb == null) {
            pb = getAllPathsBytesWithoutTemplate(t1);
            templatePathBytes.put(t1.hashCode(), pb);
          }
          for (byte[] ba : pb) {
            System.out.println(
                new String(conBytes(getCurrentPathBytes(), ba), StandardCharsets.UTF_8));
          }
        } else {
          System.out.println(getCurrentPath());
        }
      }
    }
  }

  public static List<String> getAllPaths(ISEARTNode root) {
    List<String> res = new ArrayList<>();
    DFSTraversal dfsTraversal = new DFSTraversal(root);
    Map<Integer, List<byte[]>> templatePathBytes = new HashMap<>();
    ISEARTNode node;
    while (dfsTraversal.hasNext()) {
      node = dfsTraversal.next();
      if (node.isLeaf()) {
        if (node instanceof RefNode) {
          ISEARTNode t1 = ((RefNode) node).templateRoot;
          List<byte[]> pathBytes = templatePathBytes.getOrDefault(t1.hashCode(), null);
          if (pathBytes == null) {
            pathBytes = getAllPathsBytesWithoutTemplate(t1);
            templatePathBytes.put(t1.hashCode(), pathBytes);
          }
          for (byte[] ba : pathBytes) {
            res.add(
                new String(
                    conBytes(dfsTraversal.getCurrentPathBytes(), ba), StandardCharsets.UTF_8));
          }
        } else {
          res.add(dfsTraversal.getCurrentPath());
        }
      }
    }
    return res;
  }

  // note template paths have already contained separating dot after the transaction path.
  private static List<byte[]> getAllPathsBytesWithoutTemplate(ISEARTNode root) {
    DFSTraversal dfsTraversal = new DFSTraversal(root);
    List<byte[]> res = new ArrayList<>();
    ISEARTNode node;
    while (dfsTraversal.hasNext()) {
      node = dfsTraversal.next();
      if (node instanceof RefNode) {
        throw new RuntimeException("Shall be template within this tree.");
      }

      if (node.isLeaf()) {
        res.add(dfsTraversal.getCurrentPathBytes());
      }
    }
    return res;
  }

  public static void consumeNodes(ISEARTNode root, BiConsumer<ISEARTNode, String> consumer) {
    DFSTraversal dfsTraversal = new DFSTraversal(root);
    while (dfsTraversal.hasNext()) {
      consumer.accept(dfsTraversal.next(), dfsTraversal.getCurrentPath());
    }
  }

  @Override
  public boolean hasNext() {
    while (!stack.isEmpty()) {
      if (stack.getLast().key == null) {
        stack.removeLast();
        tbl -= traceBytes.getLast().length;
        traceBytes.removeLast();
        continue;
      }
      return true;
    }
    return false;
  }

  @Override
  public ISEARTNode next() {
    KeyedNode cur = stack.removeLast();
    if (cur.key == null) {
      // shall not happen
      throw new RuntimeException();
    }

    String label =
        (char) cur.key.byteValue()
            + (cur.node.getPartialKey() == null
                ? ""
                : new String(cur.node.getPartialKey(), StandardCharsets.UTF_8));
    byte[] lb;
    if (cur.node.getPartialKey() == null) {
      lb = new byte[] {cur.key};
    } else {
      lb = new byte[1 + (cur.node.getPartialKey() == null ? 0 : cur.node.getPartialKey().length)];
      lb[0] = cur.key;
      System.arraycopy(cur.node.getPartialKey(), 0, lb, 1, cur.node.getPartialKey().length);
    }
    traceBytes.addLast(lb);
    tbl += lb.length;
    stack.addLast(new KeyedNode(null, null));

    if (cur.node.isLeaf()) {
      return cur.node;
    }

    byte[] kes = cur.node.getKeys();
    for (int i = kes.length - 1; i >= 0; i--) {
      stack.addLast(new KeyedNode(kes[i], cur.node.getChildByKeyByte(kes[i])));
    }
    return cur.node;
  }

  public String getCurrentPath() {
    return new String(getCurrentPathBytes(), StandardCharsets.UTF_8);
  }

  public byte[] getCurrentPathBytes() {
    byte[] res = new byte[tbl];
    int ofs = 0;
    for (byte[] slice : traceBytes) {
      System.arraycopy(slice, 0, res, ofs, slice.length);
      ofs += slice.length;
    }
    return res;
  }

  // concatenate byte arrays
  public static byte[] conBytes(byte[]... bal) {
    int len = 0;
    for (byte[] ba : bal) {
      len += ba.length;
    }

    byte[] res = new byte[len];
    len = 0;
    for (byte[] ba : bal) {
      System.arraycopy(ba, 0, res, len, ba.length);
      len += ba.length;
    }

    return res;
  }

  /** To support traversal as a pair-object. */
  public static class KeyedNode {
    Byte key;
    ISEARTNode node;

    public KeyedNode(Byte k, ISEARTNode n) {
      key = k;
      node = n;
    }

    public byte getKey() {
      return key;
    }

    public ISEARTNode getNode() {
      return node;
    }

    @Override
    public String toString() {
      return (char) key.byteValue() + node.toString();
    }
  }
}
