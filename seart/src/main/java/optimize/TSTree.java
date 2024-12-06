package optimize;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import optimize.nodes.INode;
import optimize.nodes.logic.LLeaf;
import optimize.nodes.logic.LNode;

public class TSTree {
  public INode root = new LNode();
  AtomicLong nodeNum = new AtomicLong(1);

  public TSTree() {}

  public long search(String p) {
    String[] path = p.split("\\.");
    INode cur = root;
    for (int i = 1; i < path.length; i++) {

      cur = cur.getChild(path[i]);
      if (cur == null) throw new RuntimeException("Key not found");
    }
    return cur.getValue();
  }

  public long insert(String p, long value) {
    String[] path = p.split("\\.");
    if (!path[0].equals("root")) {
      throw new RuntimeException("No heading root in :" + p);
    }

    INode cur = root, child;
    for (int i = 1; i < path.length; i++) {
      child = cur.getChild(path[i]);
      if (child == null) {
        nodeNum.incrementAndGet();
        child = (i == path.length - 1 ? new LLeaf(value) : new LNode());
        cur.addChild(path[i], child);
      }
      cur = child;
    }
    return nodeNum.get();
  }

  public void traversePostOrderRec(quadFunction<INode, String, INode, Deque<String>> consumer) {
    traversePostOrderRec(consumer, null, root, null, null);
  }

  public static void traversePostOrderRec(
      quadFunction<INode, String, INode, Deque<String>> consumer,
      Deque<String> trace,
      INode cur,
      INode par,
      String key) {
    if (trace == null) {
      trace = new ArrayDeque<>();
    }

    List<String> keys = cur.getKeys();
    if (keys == null || keys.isEmpty()) {
      consumer.apply(par, key, cur, trace);
      return;
    }

    for (String k : keys) {
      String label =
          k
              + (cur.getPartialKey() == null
                  ? ""
                  : new String(cur.getPartialKey(), StandardCharsets.UTF_8));
      trace.addLast(label);
      traversePostOrderRec(consumer, trace, cur.getChild(k), cur, k);
      trace.removeLast();
    }

    consumer.apply(par, key, cur, trace);
  }

  public void traversePreOrder(quadFunction<INode, String, INode, Deque<String>> consumer) {
    class KeyedNode {
      String key;
      INode node, par;

      public KeyedNode(String k, INode i, INode p) {
        key = k;
        node = i;
        par = p;
      }
    }

    Deque<KeyedNode> nodeStk = new ArrayDeque<>();
    Deque<String> trace = new ArrayDeque<>();

    nodeStk.addLast(new KeyedNode("root", root, null));

    while (!nodeStk.isEmpty()) {
      KeyedNode cur = nodeStk.removeLast();
      if (cur.key == null) {
        trace.removeLast();
        continue;
      }

      String label = cur.key + (cur.node.getPartialKey() == null ? "" : cur.node.getPartialKey());
      trace.addLast(label);
      nodeStk.addLast(new KeyedNode(null, null, null));

      List<String> keys;
      if ((keys = cur.node.getKeys()) != null) {
        for (int i = keys.size() - 1; i >= 0; i--) {
          nodeStk.addLast(new KeyedNode(keys.get(i), cur.node.getChild(keys.get(i)), cur.node));
        }
      }

      consumer.apply(cur.par, cur.key, cur.node, trace);
    }
  }

  @FunctionalInterface
  public interface quadFunction<T, U, V, R> {
    void apply(T t, U u, V v, R r);
  }

  public static void main(String[] args) {
    String[] test = {"root.sg1.d1.v1", "root.sg1.d1.v2", "root.sg2.d1.v1", "root.sg2.d3.v1"};

    TSTree tree = new TSTree();
    for (String s : test) {
      tree.insert(s, s.hashCode());
    }

    tree.traversePreOrder(
        (p, k, n, s) -> {
          System.out.printf("%s, %d ", k, s.size());
          System.out.println(String.join(".", s));
        });
  }
}
