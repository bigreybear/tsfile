package optimize;

import static optimize.nodes.cdm.CNodeHelper.bytes2Int;
import static optimize.nodes.cdm.CNodeHelper.extractBytes;
import static optimize.util.ArrayHelper.removeTrailingZeros;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import optimize.nodes.INode;
import optimize.nodes.ITSNode;
import optimize.nodes.cdm.CLeaf;
import optimize.nodes.cdm.CNode;
import optimize.nodes.cdm.CNode4;
import optimize.nodes.cdm.ICNode;
import optimize.nodes.fdm.FLeaf;
import optimize.nodes.fdm.IFNode;
import optimize.nodes.hash.HNodeV2;
import optimize.nodes.logic.LLeaf;
import optimize.nodes.logic.LLeafVDev;
import optimize.nodes.logic.LNodeVDev;
import optimize.nodes.ref.CDMRefNode;
import optimize.nodes.ref.FDMRefNode;
import optimize.nodes.ref.HashRefNode;
import optimize.util.ByteArray;

public class TSTreeVDev {
  public ITSNode root = new LNodeVDev() {};
  AtomicLong nodeNum = new AtomicLong(1);

  public TSTreeVDev() {}

  public long search(String p) {
    String[] path = p.split("\\.");
    ITSNode cur = root;
    for (int i = 1; i < path.length; i++) {

      cur = cur.getLogicalChild(path[i]);
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

      for (int i = 0; i < kbs.length; ) {
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
          // the logic is, as kbs not exhausted, res should not be a FLeaf
          if (res instanceof IFNode) cur = (IFNode) res;
          else {
            // should be in template
            if (i == kbs.length) {
              return ((FDMRefNode) res)
                  .getValFrom(path[oi + 1].getBytes(StandardCharsets.UTF_8), 0);
            } else {
              return ((FDMRefNode) res).getValFrom(kbs, i);
            }
          }
        }
      }

      if (prefixedLeaf) {
        prefixedLeaf = false;
        res = ((IFNode) res).getFValue();
        if (res instanceof LLeaf) {
          return res.getValue();
        }
        cur = (IFNode) res;
        continue;
      }

      if (directLeaf) {
        directLeaf = false;
        if (oi == path.length - 1) return cur.getFValue().getValue();
        if (cur.getFValue() instanceof FDMRefNode) {
          return ((FDMRefNode) cur.getFValue())
              .getValFrom(path[oi + 1].getBytes(StandardCharsets.UTF_8), 0);
        }
        cur = (IFNode) cur.getFValue();
        continue;
      }

      if (res != null && res.getPartialKey() == null && res instanceof IFNode) {
        if (res instanceof FLeaf) {
          res = ((FLeaf) res).getFValue();
          if (res instanceof LLeaf) {
            return res.getValue();
          }
          if (res instanceof FDMRefNode) {
            return ((FDMRefNode) res).getValFrom(path[oi + 1].getBytes(StandardCharsets.UTF_8), 0);
          }
          cur = (IFNode) res;
          continue;
        } else if (((IFNode) res).get((byte) 0) != null) {
          res = ((IFNode) res).get((byte) 0);
          cur = (IFNode) ((IFNode) res).getFValue();
          continue;
        }
      }

      if (res == null) throw new RuntimeException("Key not found");
      if (res instanceof FDMRefNode) {
        return ((FDMRefNode) res).getValFrom(path[oi + 1].getBytes(StandardCharsets.UTF_8), 0);
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
    ICNode cur = (ICNode) root;
    int channel = -1;
    int[] brPos;
    byte[] pk, curBrKeys, checkBrKeys;
    for (int oi = 1; oi < path.length; oi++) {
      final byte[] sk = path[oi].getBytes(StandardCharsets.UTF_8);
      int idx = 0;

      if (cur instanceof CLeaf) {
        pk = cur.getPartialKey();
        if (pk != null) {
          for (int j = 0; j < pk.length; j++) {
            if (pk[j] != sk[idx]) throw new RuntimeException("Inconsistent key.");
            idx++;
          }
        }

        if (idx < sk.length) throw new RuntimeException("Should exhaust partial key on CLeaf.");
        if (((CLeaf) cur).ptr instanceof LLeaf) {
          return ((CLeaf) cur).ptr.getValue();
        }
        cur = (ICNode) ((CLeaf) cur).ptr;
        continue;
      }

      while (cur instanceof CNode4 && idx < sk.length) {
        pk = cur.getPartialKey();
        if (pk != null) {
          for (int j = 0; j < pk.length; j++) {
            if (sk[idx] != pk[j]) throw new RuntimeException("Key not exists: " + path[oi]);
            idx++;
          }

          if (idx == sk.length) {
            if (cur instanceof CLeaf) {
              cur = (ICNode) ((CLeaf) cur).ptr;
              break;
            }

            channel = cur.getBrKeyIdx(0);
            cur = cur.getPtrByPos(channel);
            break;
          }
        }

        brPos = cur.getBranchingPos();
        if (brPos == null) break;

        curBrKeys = extractBytes(sk, brPos);
        channel = cur.getBrKeyIdx(bytes2Int(curBrKeys));
        if (channel < 0) throw new RuntimeException("Key not found: " + path[oi]);
        checkBrKeys = cur.assembleKeyAt(channel);
        for (int i = 0; i < checkBrKeys.length && idx < sk.length; i++) {
          if (checkBrKeys[i] != sk[idx]) throw new RuntimeException();
          idx++;
        }

        cur = cur.getPtrByPos(channel);

        // if (((CNode4) cur).ptrs[channel] != null) {
        //   cur = cur.getPtrByPos(channel);
        // } else {
        //   INode res = ((CNode)cur).ptrs[channel];
        //   if (res instanceof LLeaf) return res.getValue();
        //   else throw new UnsupportedOperationException();
        // }
      }

      if (cur instanceof CDMRefNode) {
        return ((CDMRefNode) cur).getValFrom(sk, idx);
      }

      if (idx == sk.length && !(cur instanceof CLeaf)) {
        cur = cur.getPtrByPos(0);
        if (cur instanceof CLeaf) {
          // a finaly leaf, just return the value
          if (((CLeaf) cur).ptr instanceof LLeaf) {
            return ((CLeaf) cur).ptr.getValue();
          } else {
            // the partial key must be for next segment, just continue
            if (cur.getPartialKey() != null && cur.getPartialKey().length > 0) {
              continue;
            } else {
              // no partial key, and not final, no branching (leaf), so must proceed once more
              cur = (ICNode) ((CLeaf) cur).ptr;
            }
            continue;
          }
        }
        // a prefixed node must be a leaf
        else throw new RuntimeException("Illegal route.");
      }

      if (cur instanceof CLeaf) {
        if (cur.getPartialKey() != null) {
          pk = cur.getPartialKey();
          for (int j = 0; j < pk.length; j++) {
            if (pk[j] != sk[idx]) throw new RuntimeException("Key Inconsistent");
            idx++;
          }
        }
        if (idx == sk.length) {
          // cur = (ICNode) ((CLeaf) cur).ptr;
          INode res = ((CLeaf) cur).ptr;
          if (res instanceof LLeaf) return res.getValue();
          cur = (ICNode) res;
          continue;
        } else {
          throw new UnsupportedOperationException();
        }
      }

      // if (idx == sk.length && !(cur instanceof CLeaf)) {
      //   // sk exhausted, so there is an immediate-prefix node
      //   cur = cur.getPtrByPos(cur.getBrKeyIdx(0));
      // }

      while (cur instanceof CNode && idx < sk.length) {
        int pidx = idx;
        pk = cur.getPartialKey();
        if (pk != null) {
          for (int j = 0; j < pk.length; j++) {
            if (sk[idx] != pk[j]) throw new RuntimeException("Key not consistent with partial key");
            idx++;
          }
        }

        channel = cur.getBrKeyIdx(removeTrailingZeros(extractBytes(sk, cur.getBranchingPos())));
        checkBrKeys = ((CNode) cur).assembleKeyAt(channel, pidx, sk.length);
        for (int j = 0; j < checkBrKeys.length; j++) {
          if (sk[idx] != checkBrKeys[j])
            throw new UnsupportedOperationException("Inconsistent on assemble key.");
          idx++;
        }

        if (((CNode) cur).ptrs[channel] instanceof ICNode) {
          cur = cur.getPtrByPos(channel);
        } else {
          INode res = ((CNode) cur).ptrs[channel];
          if (res instanceof LLeaf) return res.getValue();
          else throw new UnsupportedOperationException();
        }
      }

      if (idx == sk.length) {
        if (cur instanceof CLeaf) {
          if (cur.getPartialKey() != null && cur.getPartialKey().length != 0) {
            continue;
          }

          // next level as normal
          if (((CLeaf) cur).ptr instanceof LLeaf) {
            return ((CLeaf) cur).ptr.getValue();
          }
          cur = (ICNode) ((CLeaf) cur).ptr;
          continue;
        }

        if (cur instanceof CDMRefNode) {
          // next level as template
          continue;
        }
      }

      // if (idx == sk.length && !(cur instanceof CLeaf)) {
      //   // sk exhausted, so there is an immediate-prefix node
      //   cur = cur.getPtrByPos(cur.getBrKeyIdx(0));
      // }
      if (cur instanceof CDMRefNode) {
        return ((CDMRefNode) cur).getValFrom(sk, idx);
      }

      if (cur == null) throw new RuntimeException("Key not found");
    }

    byte[] sk = path[path.length - 1].getBytes(StandardCharsets.UTF_8);
    int idx = 0;
    if (cur instanceof CLeaf) {
      pk = cur.getPartialKey();
      if (pk != null) {
        for (int j = 0; j < pk.length; j++) {
          if (pk[j] != sk[idx]) throw new RuntimeException("Inconsistent key.");
          idx++;
        }
      }

      if (idx < sk.length) throw new RuntimeException("Should exhaust partial key on CLeaf.");
      if (((CLeaf) cur).ptr instanceof LLeaf) {
        return ((CLeaf) cur).ptr.getValue();
      }
      cur = (ICNode) ((CLeaf) cur).ptr;
    }

    if (cur instanceof CDMRefNode) {
      return ((CDMRefNode) cur).getValFrom(sk, idx);
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
        }
        ;

        // try all remaining key
        res = cur.children.get(new ByteArray(Arrays.copyOfRange(sk, i, sk.length)));
        if (res != null) {

          if (res instanceof HashRefNode) {
            // current segment exhausted, next node all for template
            return getValFromHashTemplate(
                (HashRefNode) res, path[_i + 1].getBytes(StandardCharsets.UTF_8), 0);
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
        res = cur.children.get(new ByteArray(Arrays.copyOfRange(sk, i, i + 1)));

        if (res instanceof HashRefNode) {
          return getValFromHashTemplate((HashRefNode) res, sk, i + 1);
        }

        cur = (HNodeV2) res;
      }
      if (res instanceof LLeaf) {
        return res.getValue();
      }
      if (res instanceof HashRefNode) {
        return getValFromHashTemplate(
            (HashRefNode) res, path[_i + 1].getBytes(StandardCharsets.UTF_8), 0);
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

    int _order =
        (int) res.template.getChildByBytes(Arrays.copyOfRange(sk, i, sk.length)).getValue();
    return res.values[_order];
  }

  public long insert(String p, long value) {
    String[] path = p.split("\\.");
    if (!path[0].equals("root")) {
      throw new RuntimeException("No heading root in :" + p);
    }

    ITSNode cur = root, child;
    for (int i = 1; i < path.length; i++) {
      child = cur.getLogicalChild(path[i]);
      if (child == null) {
        nodeNum.incrementAndGet();
        child = (i == path.length - 1 ? new LLeafVDev(value) : new LNodeVDev());
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

    TSTreeVDev tree = new TSTreeVDev();
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
