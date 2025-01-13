package optimize;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import optimize.nodes.IMicroNode;
import optimize.nodes.ITSNode;
import optimize.nodes.cdm.ICNode;
import optimize.nodes.fdm.IFNode;
import optimize.nodes.logic.LLeaf;
import optimize.nodes.logic.LNodeV3;

public class TSTree {
  public ITSNode root = new LNodeV3();
  AtomicLong nodeNum = new AtomicLong(1);
  private boolean TWO_LEVEL_PATH = true;
  private static final SearchStatus ss = new SearchStatus();

  public TSTree() {}

  public void setTwoLevelPath(boolean twoLevelPath) {
    TWO_LEVEL_PATH = twoLevelPath;
  }

  private String[] parsePathString(String p) {
    final String[] pathNodes = p.split("\\.");
    if (!pathNodes[0].equals("root")) {
      throw new RuntimeException("No heading root in :" + p);
    }
    if (TWO_LEVEL_PATH) {
      return new String[] {
        pathNodes[0], String.join(".", Arrays.copyOfRange(pathNodes, 1, pathNodes.length))
      };
    } else {
      return pathNodes;
    }
  }

  public long searchFDM(String p) {
    final String[] pathNodes = parsePathString(p);

    IFNode cur = (IFNode) root;
    ss.reset().setIfNode(cur);
    byte[] kb;
    for (int pid = 1; pid < pathNodes.length; pid++) {
      kb = pathNodes[pid].getBytes(StandardCharsets.UTF_8);
      while (!ss.isFinished()) {
        cur = cur.getFDMChild(kb, ss);
      }

      ss.setCurLen(0).setFinished(false);
    }
    return cur.getValue();
  }

  public long searchCDM(String p) {
    final String[] pathNodes = parsePathString(p);

    ICNode cur = (ICNode) root;
    ss.reset().setIcNode(cur);
    byte[] kb;
    for (int pid = 1; pid < pathNodes.length; pid++) {
      kb = pathNodes[pid].getBytes(StandardCharsets.UTF_8);
      while (!ss.isFinished()) {
        cur = cur.proceedQueryCDM(kb, ss);
      }
      // update kb, set ss.curLen to 0
      ss.setCurLen(0);
      ss.setFinished(false);
    }
    return cur.getValue();
  }

  public long searchHash(String p) {
    final String[] pathNodes = parsePathString(p);

    IMicroNode cur = (IMicroNode) root;
    ss.reset().setImNode(cur);
    byte[] kb;
    for (int pid = 1; pid < pathNodes.length; pid++) {
      kb = pathNodes[pid].getBytes(StandardCharsets.UTF_8);
      while (!ss.isFinished()) {
        cur = cur.getHashChild(kb, ss);
      }
      // update kb, set ss.curLen to 0
      ss.setCurLen(0);
      ss.setFinished(false);
    }
    return cur.getValue();
  }

  public long searchLogical(String p) {
    final String[] pathNodes = parsePathString(p);
    ITSNode cur = root;
    for (int pid = 1; pid < pathNodes.length; pid++) {
      cur = cur.getLogicalChild(pathNodes[pid]);
    }
    return cur.getValue();
  }

  public long insert(String p, long value) {
    final String[] path = parsePathString(p);

    ITSNode cur = root, child;
    for (int i = 1; i < path.length; i++) {
      child = cur.getLogicalChild(path[i]);
      if (child == null) {
        nodeNum.incrementAndGet();
        child = (i == path.length - 1 ? new LLeaf(value) : new LNodeV3());
        cur.addChild(path[i], child);
      }
      cur = child;
    }
    return nodeNum.get();
  }

  public void traversePostOrderRec(
      IQuadFunction<ITSNode, String, ITSNode, Deque<String>> consumer) {
    traversePostOrderRec(consumer, null, root, null, null);
  }

  public static void traversePostOrderRec(
      IQuadFunction<ITSNode, String, ITSNode, Deque<String>> consumer,
      Deque<String> trace,
      ITSNode cur,
      ITSNode par,
      String key) {
    if (trace == null) {
      trace = new ArrayDeque<>();
    }

    List<String> keys = cur.getStringKeys();
    if (keys == null || keys.isEmpty()) {
      consumer.apply(par, key, cur, trace);
      return;
    }

    for (String k : keys) {
      String label =
          k + (cur.getParKey() == null ? "" : new String(cur.getParKey(), StandardCharsets.UTF_8));
      trace.addLast(label);
      traversePostOrderRec(consumer, trace, cur.getLogicalChild(k), cur, k);
      trace.removeLast();
    }

    consumer.apply(par, key, cur, trace);
  }

  public void traversePreOrder(IQuadFunction<ITSNode, String, ITSNode, Deque<String>> consumer) {
    class KeyedNode {
      String key;
      ITSNode node, par;

      public KeyedNode(String k, ITSNode i, ITSNode p) {
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

      String label = cur.key + (cur.node.getParKey() == null ? "" : cur.node.getParKey());
      trace.addLast(label);
      nodeStk.addLast(new KeyedNode(null, null, null));

      List<String> keys;
      if ((keys = cur.node.getStringKeys()) != null) {
        for (int i = keys.size() - 1; i >= 0; i--) {
          nodeStk.addLast(
              new KeyedNode(keys.get(i), cur.node.getLogicalChild(keys.get(i)), cur.node));
        }
      }

      consumer.apply(cur.par, cur.key, cur.node, trace);
    }
  }

  @FunctionalInterface
  public interface IQuadFunction<T, U, V, R> {
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
