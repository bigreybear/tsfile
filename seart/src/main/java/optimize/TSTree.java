package optimize;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import optimize.nodes.INode;
import optimize.nodes.cdm.CNodeHelper;
import optimize.nodes.fdm.FLeaf;
import optimize.nodes.fdm.IFNode;
import optimize.nodes.hash.HNodeV2;
import optimize.nodes.logic.LLeaf;
import optimize.nodes.logic.LNode;
import optimize.nodes.ref.FDMRefNode;
import optimize.nodes.ref.HashRefNode;
import optimize.util.ByteArray;

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

  public long searchFDM(String p) {
    String[] path = p.split("\\.");
    IFNode cur = (IFNode) root;
    INode res = cur;
    byte[] pk;

    boolean directLeaf = false, prefixedLeaf = false, resTemplate = false;
    for (int oi = 1; oi < path.length; oi++) {
      byte[] kbs = path[oi].getBytes(StandardCharsets.UTF_8);
      pk = cur.getPartialKey();

      for (int i =0; i < kbs.length; ) {
        pk = cur.getPartialKey();
        i += IFNode.matchLen(pk, kbs, i);

        if (i == kbs.length && cur instanceof FLeaf) {
          directLeaf = true;
          break;
        }

        if (i == kbs.length && !(cur instanceof FLeaf)) {
          res = cur.get((byte) 0);
          prefixedLeaf = true;
          break;
        }

        if (i < kbs.length) {
          res = cur.get(kbs[i]);
          i++;
          if (res instanceof IFNode) cur = (IFNode) res;
          else {
            // should be in template
            if (i == kbs.length) {
              return  ((FDMRefNode)res).getValFrom(path[oi+1].getBytes(StandardCharsets.UTF_8), 0);
            } else {
              return  ((FDMRefNode)res).getValFrom(kbs, i);
            }
          }
        }
      }

      if (prefixedLeaf) {
        prefixedLeaf = false;
        res = ((IFNode)res).getFValue();
        if (res instanceof LLeaf) {
          return res.getValue();
        }
        cur = (IFNode) res;
        continue;
      }

      if (directLeaf) {
        directLeaf = false;
        if (oi == path.length - 1) return cur.getFValue().getValue();
        cur = (IFNode) cur.getFValue();
        continue;
      }

      if (res != null && res.getPartialKey() == null && res instanceof IFNode) {
        if (((IFNode) res).get((byte) 0) != null) {
          res = ((IFNode) res).get((byte) 0);
          cur = (IFNode) ((IFNode) res).getFValue();
          continue;
        } else if (res instanceof FLeaf) {
          res = ((FLeaf) res).getFValue();
          if (res instanceof LLeaf) {
            return res.getValue();
          }
          cur = (IFNode) res;
          continue;
        }
      }

      if (res == null) throw new RuntimeException("Key not found");
      if (res instanceof FDMRefNode) {
        return ((FDMRefNode) res).getValFrom(path[oi+1].getBytes(StandardCharsets.UTF_8), 0);
      }

      if (cur instanceof FLeaf) {
        if (cur.getFValue() instanceof LLeaf) return cur.getFValue().getValue();
        cur = (IFNode) cur.getFValue();
      }
    }
    return cur.getValue();
  }

  public long searchCDM(String p) {
    String[] path = p.split("\\.");
    INode cur = root;
    for (int i = 1; i < path.length; i++) {

      cur = cur.getChild(path[i]);
      if (cur == null) throw new RuntimeException("Key not found");
    }
    return cur.getValue();
  }

  public long searchHash(String p) {
    String[] path = p.split("\\.");
    HNodeV2 cur = (HNodeV2) root;
    INode res = null;
    ByteArray EMPTY_ARRAY = new ByteArray(new byte[0]);
    byte[] pk;
    for (int _i = 1; _i < path.length; _i++) {

      // cur = cur.getChild(path[i]);
      // inlined
      final byte[] sk = path[_i].getBytes(StandardCharsets.UTF_8);
      for (int i = 0; i < sk.length; i++) {
        pk = cur.getPartialKey();
        if (pk != null) {
          for (int j = 0; j < pk.length; j++) {
            if (sk[i] == pk[j]) i++;
            else throw new RuntimeException("Key not consistent on Partial key.");
          }
        }

        if (cur.children == null) throw new RuntimeException("Null chilren.");
        if (i == sk.length) {
          // prefixed child
          res = cur.children.get(EMPTY_ARRAY);
          break;
        };

        // try all remaining key
        res = cur.children.get(new ByteArray(Arrays.copyOfRange(sk, i, sk.length)));
        if (res != null) {

          if (res instanceof HashRefNode) {
            // current segment exhausted, next node all for template
            return getValFromHashTemplate((HashRefNode) res, path[_i+1].getBytes(StandardCharsets.UTF_8), 0);
          }

          // Note(zx) sk exhausted, if the cur node has zero-len key, then that is the target
          //  meaning, there are some sibling prefixing the search key
          if (res instanceof HNodeV2) {
            if (res.getPartialKey() == null && ((HNodeV2) res).children.containsKey(EMPTY_ARRAY)) {
              res = ((HNodeV2) res).children.get(EMPTY_ARRAY);
              break;
            }
          }
          break;
        }

        // no remaining, use first byte
        res = cur.children.get(new ByteArray(Arrays.copyOfRange(sk, i, i+1)));

        if (res instanceof HashRefNode) {
          return getValFromHashTemplate((HashRefNode) res, sk, i+1);
        }

        cur = (HNodeV2) res;
      }
      if (res instanceof LLeaf) {
        return res.getValue();
      }
      if (res instanceof HashRefNode) {
        return getValFromHashTemplate((HashRefNode) res, path[_i+1].getBytes(StandardCharsets.UTF_8), 0);
      }
      cur = (HNodeV2) res;
      // inlined end

      if (cur == null) throw new RuntimeException("Key not found");
    }
    return cur.getValue();
  }

  private static long getValFromHashTemplate(HashRefNode res, byte[] sk, int i) {
    byte[] _pk = res.pk;
    for (int _in = 0; _pk != null && _in < _pk.length; _in++) {
      if (_pk[_in] == sk[i]) i++;
      else throw new RuntimeException("Key not consistent on Partial key.");
    }

    int _order = (int) res.template.getChildByBytes(
        Arrays.copyOfRange(sk, i, sk.length)
    ).getValue();
    return res.values[_order];
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

  public void traversePostOrderRec(IQuadFunction<INode, String, INode, Deque<String>> consumer) {
    traversePostOrderRec(consumer, null, root, null, null);
  }

  public static void traversePostOrderRec(
      IQuadFunction<INode, String, INode, Deque<String>> consumer,
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

  public void traversePreOrder(IQuadFunction<INode, String, INode, Deque<String>> consumer) {
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
