package optimize.merge.skeleton;

import optimize.nodes.ITSNode;
import optimize.nodes.cdm.ICNode;
import optimize.nodes.logic.LLeaf;
import optimize.nodes.logic.LLeafAnnotated;
import optimize.util.ByteArray;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static optimize.nodes.cdm.CNodeHelper.chooseCNodes;

/**
 * Purposes: <p>
 * 1. Converting a group of IBNodes within a mini tree into an object based on infoObj; {@link #transform} <br>
 * 2. For high-degree nodes, aligning the number of pos in each branch in preparation for conversion to hash;
 *  {@link #alignBranches}<br>
 * 2.1 This process may generate small nodes, and attempts are made to merge them with their children; <br>
 * 3. Converting each rep object into CNodes. {@link #transformToCNodes()}<br>
 */
public class MiniTreeRep {
  int hl = 0;
  public byte[] parKey; // equals par key of mini-root
  TreeSet<Integer> pos = new TreeSet<>();
  // the full mini-key does NOT include the par key of mini-root
  public TreeMap<ByteArray, Object> fullKeyMap = new TreeMap<>();


  /**
   * Top-down merge. <br>
   * only fix case where parent can be easily (without pickup) merged into its children.
   */
  public void mergeDownward() {
    MiniTreeRep chdRep;
    boolean tryToMerge = pos.size() <= 2 && fullKeyMap.size() < 8;
    if (tryToMerge) {
      // check, merge and update children
      TreeSet<Integer> union = new TreeSet<>(pos);
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

          // todo Note(zx) further optimization: merge for better perf. but more space.
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

  /**
   * Note(zx) Inverse to {@link optimize.merge.bum.PullUpSharedPosition#pushDownMiniRoot}. <br>
   * Update map within its parent.
   * Three cases: <br>
   * 1) tp within partial key, just update the parent map; <br>
   * 2) tp is between current positions, remove original entry and replace with several more; <br>
   * 3) tp is behind the last position, recur on related child, and the return key should contains.
   *
   * @param parRep parental rep object.
   * @param key pointing to this rep from parent rep.
   * @param tp target position to be included by parent.
   * @return the map to REPLACE original entry, current instance will be obsoleted.
   */
  private Map<ByteArray, Object> includeTo(MiniTreeRep parRep, ByteArray key, int tp) {
    int firstBr = pos.first(); // the position of the first byte of keys within the map
    int ofs = tp - firstBr;
    int truncateLen = ofs + parKey.length + 1;
    TreeMap<ByteArray, Object> tmpMap = new TreeMap<>();

    // partial key is long enough: only needs to update key and partial key
    if (ofs < 0) {
      // truncate the partial key, append previous part to the key of parent map
      ByteArray newKey = new ByteArray(
          key.getVal(), Arrays.copyOfRange(parKey, 0, truncateLen));
      parKey = truncateLen == parKey.length
          ? null
          : Arrays.copyOfRange(parKey, parKey.length + ofs, parKey.length);
      tmpMap.put(newKey, this); // this instance only updates partial key
      return tmpMap;
    }

    ByteArray prefix = new ByteArray(key.getVal(), parKey);
    ByteArray nk, lk;  // new key, left key
    Object r;
    List<MiniTreeRep> newReps = new ArrayList<>(); // to calculate positions later
    for (Map.Entry<ByteArray, Object> entry : fullKeyMap.entrySet()) {
      // exactly extended
      if (entry.getKey().getVal().length == truncateLen) {
        tmpMap.put(
            new ByteArray(prefix.getVal(), entry.getKey().getVal()),
            entry.getValue());
        continue;
      }

      if (entry.getKey().getVal().length > truncateLen) {
        // 截取的 key 只是原先 key 的一部分，不需去孩子节点中找更多 key。
        // 两种情况：
        // 1. 截取前缀有多个 key
        // 2. 只有单个 key
        // 无论如何，构造一个中间 miniRep。对于情况 2，后续merge 可将其合并
        // 这里新增的节点最后要统计计算 pos
        nk = new ByteArray(prefix.getVal(), Arrays.copyOfRange(entry.getKey().getVal(), 0, truncateLen));
        lk = new ByteArray(Arrays.copyOfRange(entry.getKey().getVal(), truncateLen, entry.getKey().getVal().length));

        if (entry.getValue() instanceof LLeaf leaf) {
          leaf.setParKey(
              leaf.getParKey() == null ? lk.getVal() : concatenate(leaf.getParKey(), lk.getVal())
          );
          tmpMap.put(nk, leaf);
          continue;
        }

        if ((r = tmpMap.get(nk)) == null) {
          MiniTreeRep rep = new MiniTreeRep();
          newReps.add(rep);
          rep.fullKeyMap.put(lk, entry.getValue());
          tmpMap.put(nk, rep);
        } else if (r instanceof MiniTreeRep rep) {
          // more than one entry sharing the same truncated key
          rep.fullKeyMap.put(lk, entry.getValue());
        } else {throw new RuntimeException();}
        continue;
      }

      // key is not long enough, need retrieve bytes from child
      // entry.getKey().getVal().length < truncateLen
      if (entry.getValue() instanceof LLeaf leaf) {
        nk = new ByteArray(prefix.getVal(), entry.getKey().getVal());
        byte[] pk = leaf.getParKey();
        if (pk == null) {
          tmpMap.put(nk, leaf);
        } else {
          // todo check if partial key long enough

        }
        tmpMap.put(
            leaf.getParKey() == null
                ? new ByteArray(nk.getVal())
                : new ByteArray(nk.getVal(), Arrays.copyOfRange(leaf.getParKey(), 0, truncateLen)),
            entry.getValue()
        );
      }

      // child is a miniTree
      ((MiniTreeRep)entry.getValue()).includeTo(this, entry.getKey(), tp);

    }


    // append related positions to parent


    return null;
  }

  // region Static Methods

  /**
   * Align positions between branches. <br>
   * Unlike {@link optimize.merge.bum.PullUpSharedPosition#pushDownMiniRoot} in two ways: <br>
   *  1. align by sealing out all nodes below specific height rather than choosing a particular node; <br>
   *  2. recursively pull up positions within the range, rather than only direct mini-tree. <br>
   * The range is confined by the first and the last pos within the passing in rep.
   */
  public static void alignBranches(MiniTreeRep rep, Deque<MiniTreeRep> _que) {
    Deque<MiniTreeRep> que = _que == null ? new ArrayDeque<>() : _que;

    // Note(zx) Why align by including pos below rather than cut pos within?
    // We align for its big fanout. If we cut, the nodes may fall into several sorted ones.

    // Currently only a simple way:
    // 1. if fanout big, pull up and check pos num; transect a HCNode4 if too big fanout or many pos.
    // 2. if fanout small, check if pos num small: if small, try merge.
    // 3. recurs on its children
  }

  // a top-down BFS to transform each mini tree into a rep object
  public static MiniTreeRep transform(ITSNode root) {
    final Deque<MiniTreeRep> processQueue = new ArrayDeque<>();
    MiniTreeRep rootRep = transformSingle(root, processQueue);
    MiniTreeRep rep;
    while (!processQueue.isEmpty()) {
      rep = processQueue.removeFirst();
      for (Map.Entry<ByteArray, Object> entry : rep.fullKeyMap.entrySet()) {
        Object ptr = entry.getValue();
        if (ptr instanceof IBNode) {
          // replace the original BNodes mini tree with a MiniTreeRep object
          rep.fullKeyMap.put(entry.getKey(), transformSingle((ITSNode) ptr, processQueue));
        }
      }
    }
    return rootRep;
  }

  /**
   * From a mini-tree to a representation (mini full key -> child). <br>
   * If any child is another mini tree, the formed rep will be added to a static queue.
   */
  private static MiniTreeRep transformSingle(ITSNode root, final Deque<MiniTreeRep> processQueue) {
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

    // a non-recur DFS to compute the full key within the mini tree
    // the stack logs the bytes from the mini root to the visiting node.
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

  // region Utils

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

  public static byte[] concatenate(byte[] ...a) {
    int len = 0, pos = 0;
    for (byte[] ba : a) {
      len += ba.length;
    }
    byte[] res = new byte[len];
    for (byte[] ba : a) {
      System.arraycopy(ba, 0, res, pos, ba.length);
      pos += ba.length;
    }
    return res;
  }

  // endregion

  // region Naive Methods

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

  // endregion
}
