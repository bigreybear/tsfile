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

  Deque<String> trace = new ArrayDeque<>();
  Deque<KeyedNode> stack = new ArrayDeque<>();

  public DFSTraversal(ISEARTNode node) {
    root = node;
    byte[] ks = root.getKeys();
    for (int i = ks.length - 1; i >= 0; i--) {
      stack.addLast(new KeyedNode(ks[i], root.getChildByKeyByte(ks[i])));
    }
    if (root.getPartialKey() != null) {
      trace.addLast(new String(root.getPartialKey(), StandardCharsets.UTF_8));
    }
  }

  public void reset() {
    trace.clear();
    stack.clear();
    byte[] ks = root.getKeys();
    for (int i = ks.length - 1; i >= 0; i--) {
      stack.addLast(new KeyedNode(ks[i], root.getChildByKeyByte(ks[i])));
    }
    if (root.getPartialKey() != null) {
      trace.addLast(new String(root.getPartialKey(), StandardCharsets.UTF_8));
    }
  }

  public static void printAllPathsInStatic(SEARTree tree) {
    printAllPathsInStatic(tree.root);
  }

  public static void printAllPathsInStatic(ISEARTNode root) {
    DFSTraversal dfsTraversal = new DFSTraversal(root);
    Map<Integer, List<String>> templatePaths = new HashMap<>();
    ISEARTNode node;
    while (dfsTraversal.hasNext()) {
      node = dfsTraversal.next();
      if (node.isLeaf()) {
        if (node instanceof RefNode) {
          ISEARTNode t1 = ((RefNode) node).templateRoot;
          List<String> tb = templatePaths.getOrDefault(t1.hashCode(), null);
          if (tb == null) {
            tb = getAllPathsWithoutTemplate(t1);
            templatePaths.put(t1.hashCode(), tb);
          }
          for (String s : tb) {
            System.out.println(dfsTraversal.getCurrentPath() + s);
          }
        } else {
          System.out.println(dfsTraversal.getCurrentPath());
        }
      }
    }
  }

  public void printAllPaths() {
    Map<Integer, List<String>> templatePaths = new HashMap<>();
    ISEARTNode node;
    while (hasNext()) {
      node = next();
      if (node.isLeaf()) {
        if (node instanceof RefNode) {
          ISEARTNode t1 = ((RefNode) node).templateRoot;
          List<String> tb = templatePaths.getOrDefault(t1.hashCode(), null);
          if (tb == null) {
            tb = getAllPathsWithoutTemplate(t1);
            templatePaths.put(t1.hashCode(), tb);
          }
          for (String s : tb) {
            System.out.println(getCurrentPath() + s);
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
    Map<Integer, List<String>> templatePaths = new HashMap<>();
    ISEARTNode node;
    while (dfsTraversal.hasNext()) {
      node = dfsTraversal.next();
      if (node.isLeaf()) {
        if (node instanceof RefNode) {
          ISEARTNode t1 = ((RefNode) node).templateRoot;
          List<String> tb = templatePaths.getOrDefault(t1.hashCode(), null);
          if (tb == null) {
            tb = getAllPathsWithoutTemplate(t1);
            templatePaths.put(t1.hashCode(), tb);
          }
          for (String s : tb) {
            res.add(dfsTraversal.getCurrentPath() + s);
          }
        } else {
          res.add(dfsTraversal.getCurrentPath());
        }
      }
    }
    return res;
  }

  // note template paths have already contained separating dot after the transaction path.
  private static List<String> getAllPathsWithoutTemplate(ISEARTNode root) {
    DFSTraversal dfsTraversal = new DFSTraversal(root);
    List<String> res = new ArrayList<>();
    ISEARTNode node;
    while (dfsTraversal.hasNext()) {
      node = dfsTraversal.next();
      if (node instanceof RefNode) {
        throw new RuntimeException("Shall be template within this tree.");
      }

      if (node.isLeaf()) {
        res.add(dfsTraversal.getCurrentPath());
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
        trace.removeLast();
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
    trace.addLast(label);
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
    return String.join("", trace);
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
