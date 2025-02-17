package optimize.merge.skeleton;

import optimize.nodes.ITSNode;
import optimize.nodes.cdm.ICNode;
import optimize.nodes.logic.LLeaf;
import optimize.nodes.logic.LLeafAnnotated;
import optimize.util.ByteArray;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static optimize.nodes.cdm.CNodeHelper.chooseCNodes;

public class MiniTreeRep {
  int hl = 0;
  public byte[] parKey; // equals par key of mini-root
  Set<Integer> pos = new TreeSet<>();
  // the full mini-key does NOT include the par key of mini-root
  public TreeMap<ByteArray, Object> fullKeyMap = new TreeMap<>();

  public final void markHyperLevel() {
    for (Object chd : fullKeyMap.values()) {
      if (chd instanceof LLeaf) {
        hl = Math.max(hl, 1);
        continue;
      }

      ((MiniTreeRep)chd).markHyperLevel();
    }

    for (Object chd : fullKeyMap.values()) {
      if (chd instanceof MiniTreeRep) {
        hl = Math.max(hl, ((MiniTreeRep)chd).hl + 1);
      }
    }
  }

  /**
   * Top-down merge. <br>
   * only fix case where parent can be easily (without pickup) merged into its children.
   */
  public void mergeDownward() {
    MiniTreeRep chdRep;
    boolean tryToMerge = pos.size() <= 2 && fullKeyMap.size() < 8;
    if (tryToMerge) {
      // check, merge and update children
      Set<Integer> union = new TreeSet<>(pos);
      for (Object chd : fullKeyMap.values()) {
        if (chd instanceof MiniTreeRep) {
          chdRep = (MiniTreeRep) chd;
          union.addAll(chdRep.pos);
        }
      }

      TreeMap<ByteArray, Object> fkm = new TreeMap<>();
      if (union.size() < 8) {
        for (Map.Entry<ByteArray, Object> entry: fullKeyMap.entrySet()) {
          Object chd = entry.getValue();

          if (chd instanceof LLeaf) {
            fkm.put(entry.getKey(), entry.getValue());
            continue;
          }

          if (chd instanceof MiniTreeRep) {
            chdRep = (MiniTreeRep) chd;
            byte[] cpk = chdRep.parKey;
            for (Map.Entry<ByteArray, Object> iet : chdRep.fullKeyMap.entrySet()) {
              ByteArray nfk = concateByteArray(entry.getKey(), cpk, iet.getKey());
              fkm.put(nfk, iet.getValue());
            }
          }
        }
      }

      pos = union;
      fullKeyMap = fkm;
    }

    // traverse children
    for (Object chd : fullKeyMap.values()) {
      if (chd instanceof MiniTreeRep) {
        ((MiniTreeRep) chd).mergeDownward();
      }
    }
  }

  private ByteArray concateByteArray(ByteArray k1, byte[] pk, ByteArray k2) {
    int len = k1.getVal().length + k2.getVal().length + (pk == null ? 0 : pk.length), pos = 0;
    byte[] res = new byte[len];
    System.arraycopy(k1.getVal(), 0, res, pos, k1.getVal().length);
    pos += k1.getVal().length;
    if (pk != null) {
      System.arraycopy(pk, 0, res, pos, pk.length);
      pos += pk.length;
    }
    System.arraycopy(k2.getVal(), 0, res, pos, k2.getVal().length);
    return new ByteArray(res);
  }

  public ICNode transformToCNodes() {
    int[] posArr = pos.stream().mapToInt(Integer::intValue).toArray();
    int fo = fullKeyMap.size();
    ICNode node = chooseCNodes(posArr, fo);
    node.fillContent(this, posArr);
    return node;
  }

  // region Static Methods

  public static final Deque<MiniTreeRep> processQueue = new ArrayDeque<>();
  public static MiniTreeRep transform(ITSNode root) {
    MiniTreeRep rootRep = transformSingle(root);
    MiniTreeRep rep;
    while (!processQueue.isEmpty()) {
      rep = processQueue.removeFirst();
      for (Map.Entry<ByteArray, Object> entry : rep.fullKeyMap.entrySet()) {
        Object ptr = entry.getValue();
        if (ptr instanceof IBNode) {
          rep.fullKeyMap.put(entry.getKey(), transformSingle((ITSNode) ptr));
        }
      }
    }
    return rootRep;
  }

  /**
   * From a mini-tree to a map (mini full key -> child). <br>
   * If any child is another mini tree, the formed rep will be added to a static queue.
   */
  private static MiniTreeRep transformSingle(ITSNode root) {
    Deque<Byte> keyStk = new ArrayDeque<>(); // key byte that indexes the node in nodeStk
    Deque<ITSNode> nodeStk = new ArrayDeque<>(); // parallel to keyStk

    final ITSNode NODE_PAD = new LLeaf(-1);
    final byte[] RMK_PAD = new byte[0];

    ITSNode curNode, child;
    byte curKey;
    byte[] curRmk;

    MiniTreeRep rep = new MiniTreeRep();
    rep.parKey = root.getParKey();
    rep.pos.add(root.getInfoObj().brPos);

    final Map<ByteArray, Object> fullKeyMap = new TreeMap<>();

    // no need to make full key from stack
    for (byte k : root.getInfoObj().getKeys()){
      curNode = root.getInfoObj().getChd(k);
      if (curNode.isLogicalLeaf() || curNode.getInfoObj().isMiniRoot) {
        fullKeyMap.put(new ByteArray(k), curNode);
        continue;
      }

      keyStk.addLast(k);
      nodeStk.addLast(curNode);
    }

    Deque<byte[]> fullKeyStk = new ArrayDeque<>();
    while (!nodeStk.isEmpty()) {
      curNode = nodeStk.removeLast();

      if (curNode == NODE_PAD) {
        fullKeyStk.removeLast();
        fullKeyStk.removeLast();
        continue;
      }

      curKey = keyStk.removeLast();
      fullKeyStk.addLast(new byte[] {curKey});
      fullKeyStk.addLast((curRmk = curNode.getParKey()) == null ? RMK_PAD : curRmk);
      nodeStk.addLast(NODE_PAD);
      rep.pos.add(curNode.getInfoObj().brPos);
      for (byte k : curNode.getInfoObj().getKeys()) {
        child = curNode.getInfoObj().getChd(k);
        if (child.isLogicalLeaf() || child.getInfoObj().isMiniRoot) {
          fullKeyMap.put(makeFullKey(fullKeyStk, k), child);
          continue;
        }

        keyStk.addLast(k);
        nodeStk.addLast(child);
      }
    }

    boolean hasMiniTreeChild = false;
    for (Map.Entry<ByteArray, Object> entry : fullKeyMap.entrySet()) {
      if (entry.getValue() instanceof LLeafAnnotated) {
        LLeafAnnotated oLeaf = (LLeafAnnotated) entry.getValue();
        LLeaf nLeaf = new LLeaf(oLeaf.getValue());
        nLeaf.setParKey(oLeaf.getParKey());
        rep.fullKeyMap.put(entry.getKey(), nLeaf);
        continue;
      }
      hasMiniTreeChild = true;
      rep.fullKeyMap.put(entry.getKey(), entry.getValue());
    }
    if (hasMiniTreeChild) processQueue.addLast(rep);
    return rep;
  }

  public static boolean checkCorrectness(MiniTreeRep cur, Deque<byte[]> _fks) {
    Deque<byte[]> fullKeyStk = _fks != null ? _fks : new ArrayDeque<>();

    if (cur.parKey != null) {
      fullKeyStk.addLast(cur.parKey);
    }

    for (Map.Entry<ByteArray, Object> entry : cur.fullKeyMap.entrySet()) {
      if (entry.getValue() instanceof LLeaf) {
        byte[] fk = checkFullKey(fullKeyStk, entry.getKey(), ((LLeaf)entry.getValue()).getParKey());
        long fkHash = new String(fk, StandardCharsets.UTF_8).hashCode();
        long ans = ((LLeaf)entry.getValue()).getValue();
        if (fkHash != ans)
          return false;
        continue;
      }

      MiniTreeRep _rep = (MiniTreeRep) entry.getValue();
      fullKeyStk.addLast(entry.getKey().getVal());
      if (!checkCorrectness(_rep, fullKeyStk)) return false;
      fullKeyStk.removeLast();
    }

    if (cur.parKey != null) {
      fullKeyStk.removeLast();
    }

    return true;
  }

  // endregion

  public static ByteArray makeFullKey(Deque<byte[]> stk, byte k) {
    int pos = 0, len = stk.stream().mapToInt(i->i.length).sum() + 1;
    byte[] res = new byte[len];
    for (byte[] ba: stk) {
      System.arraycopy(ba, 0, res, pos, ba.length);
      pos += ba.length;
    }
    res[res.length - 1] = k;
    return new ByteArray(res);
  }

  public static byte[] checkFullKey(Deque<byte[]> fkStk, ByteArray fk, byte[] pk) {
    final byte[] root = new byte[] {114, 111, 111, 116, 46};  // "root." is truncated within TsTree.insert
    int pos = 5, len = fkStk.stream().mapToInt(i->i.length).sum() + fk.getVal().length + 5;
    if (pk != null) len += pk.length;

    byte[] res = new byte[len];
    for (byte[] ba: fkStk) {
      System.arraycopy(ba, 0, res, pos, ba.length);
      pos += ba.length;
    }
    System.arraycopy(fk.getVal(), 0, res, pos, fk.getVal().length);
    pos += fk.getVal().length;
    if (pk != null) System.arraycopy(pk, 0, res, pos, pk.length);
    System.arraycopy(root, 0, res, 0, 5);
    return res;
  }

}
