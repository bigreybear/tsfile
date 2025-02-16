package optimize.merge.skeleton;

import optimize.nodes.ITSNode;
import optimize.nodes.logic.LLeaf;
import optimize.nodes.logic.LLeafAnnotated;
import optimize.util.ByteArray;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class MiniTreeRep {
  int hl = 0;
  byte[] parKey; // equals par key of mini-root
  Set<Integer> pos = new TreeSet<>();
  // the full mini-key does NOT include the par key of mini-root
  Map<ByteArray, Object> fullKeyMap = new TreeMap<>();

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
        rep.fullKeyMap.put(entry.getKey(), new LLeaf(((LLeafAnnotated)entry.getValue()).getValue()));
        continue;
      }
      hasMiniTreeChild = true;
      rep.fullKeyMap.put(entry.getKey(), entry.getValue());
    }
    if (hasMiniTreeChild) processQueue.addLast(rep);
    return rep;
  }

  public boolean checkCorrectness() {
    //todo
    return true;
  }

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

}
