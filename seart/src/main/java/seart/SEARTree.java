package seart;

import static seart.ISEARTNode.getMatchLength;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import seart.exception.PrefixPropertyException;
import seart.traversal.DFSTraversal;

public class SEARTree implements SeriesIndexTree, Serializable {
  public ISEARTNode root;

  public SEARTree() {}

  public SEARTree(ISEARTNode root) {
    this.root = root;
  }

  @Override
  public void insert(String key, long value) {
    insert(key.getBytes(StandardCharsets.UTF_8), value);
  }

  public void insert(byte[] insKey, long value) {
    root = insert(root, insKey, value);
  }

  // return the new root
  public static ISEARTNode insert(ISEARTNode root, byte[] insKey, long value) {
    if (root == null) {
      return new Leaf(insKey, value);
    }

    ISEARTNode curNode = root, parNode = null;
    int parNodeIdx = -1; // index of array within parent node

    // ofs for offset processing on the insert key
    int ofs = 0, matLen, nxtPtrIdx;
    while (!curNode.isLeaf()) {
      matLen = getMatchLength(curNode.getPartialKey(), insKey, ofs);
      if (ofs + matLen == insKey.length) {
        throw new PrefixPropertyException(insKey);
      }

      // if matLen==0 && pk.len != 0: split and create a Node4 at once
      if (matLen < curNode.getPartialKey().length) {
        return updateRoot(
            root, parNode, parNodeIdx, splitPartialKey(insKey, curNode, ofs, matLen, value));
      }

      nxtPtrIdx = curNode.getPtrIdxByByte(insKey[matLen + ofs]);
      if (nxtPtrIdx >= 0) {
        ofs += matLen + 1; // plus one for the branching byte
        parNode = curNode;
        parNodeIdx = nxtPtrIdx;
        curNode = curNode.getChildByPtrIndex(nxtPtrIdx);
        continue;
      }

      // matLen == pk.len : pk exhausted, insKey not, and find NO branching byte
      ISEARTNode nl = new Leaf(Arrays.copyOfRange(insKey, ofs + matLen + 1, insKey.length), value);
      ISEARTNode expNode = curNode.insert(insKey[ofs + matLen], nxtPtrIdx, nl);

      if (expNode == null) {
        return root;
      }

      if (parNode == null) {
        return expNode;
      }

      parNode.setChildPtrByIndex(parNodeIdx, expNode);
      return root;
    }

    if (curNode instanceof RefNode) {
      throw new UnsupportedOperationException(
          "Shall not insert on RefNode:" + new String(insKey, StandardCharsets.UTF_8));
    }

    matLen = getMatchLength(curNode.getPartialKey(), insKey, ofs);
    if (matLen < curNode.getPartialKey().length && matLen + ofs < insKey.length) {
      // split partial key
      return updateRoot(
          root, parNode, parNodeIdx, splitPartialKey(insKey, curNode, ofs, matLen, value));
    }
    throw new PrefixPropertyException(insKey);
  }

  // serves insert process
  private static SEARTNode splitPartialKey(
      byte[] ik, ISEARTNode cur, int ofs, int overLen, long value) {
    // from ofs+overLen+1 for 1 byte as branching key in the new Node4
    Leaf leaf = new Leaf(Arrays.copyOfRange(ik, ofs + overLen + 1, ik.length), value);
    SEARTNode n4 =
        new Node4(
            Arrays.copyOfRange(ik, ofs, ofs + overLen),
            cur.getPartialKey()[overLen],
            cur,
            ik[ofs + overLen],
            leaf);
    cur.reassignPartialKey(
        Arrays.copyOfRange(cur.getPartialKey(), overLen + 1, cur.getPartialKey().length));
    return n4;
  }

  private static ISEARTNode updateRoot(
      final ISEARTNode root, ISEARTNode par, int parIdx, SEARTNode n4) {
    if (par != null) {
      par.setChildPtrByIndex(parIdx, n4);
      return root;
    }
    return n4;
  }

  @Override
  public long search(String sk) {
    return search(sk.getBytes(StandardCharsets.UTF_8));
  }

  public long search(byte[] sk) {
    return search(this.root, sk, 0);
  }

  private volatile long lastRec;

  public long[][] analyzeSearch(String path) {
    List<Long> nanoLatencyByLevel = new ArrayList<>(100);
    List<Long> parLenByLevel = new ArrayList<>(100);
    List<Long> nodeTypeByLevel = new ArrayList<>(100);
    List<Long> value = new ArrayList<>();

    int ofs = 0, matLen, nxtPtrIdx;
    int[] res;
    byte[] sk = path.getBytes(StandardCharsets.UTF_8);
    ISEARTNode curNode = root;

    lastRec = System.nanoTime();
    while (!curNode.isLeaf()) {
      matLen = getMatchLength(curNode.getPartialKey(), sk, ofs);
      nxtPtrIdx = curNode.getPtrIdxByByte(sk[ofs + matLen]);

      if (nxtPtrIdx >= 0) {
        ofs += matLen + 1;
        curNode = curNode.getChildByPtrIndex(nxtPtrIdx);

        nanoLatencyByLevel.add(System.nanoTime() - lastRec);
        parLenByLevel.add((long) curNode.getPartialKey().length);
        if (curNode instanceof Node4) {
          nodeTypeByLevel.add(4L);
        } else if (curNode instanceof Node16) {
          nodeTypeByLevel.add(16L);
        } else if (curNode instanceof Node48) {
          nodeTypeByLevel.add(48L);
        } else if (curNode instanceof Node256) {
          nodeTypeByLevel.add(256L);
        }
        lastRec = System.nanoTime();
        continue;
      }

      throw new RuntimeException("Key not exists");
    }

    // todo faster check
    if (curNode instanceof RefNode) {
      matLen = getMatchLength(curNode.getPartialKey(), sk, ofs);

      if (matLen < curNode.getPartialKey().length) {
        throw new RuntimeException("Key not exists: " + new String(sk, StandardCharsets.UTF_8));
      }
      nanoLatencyByLevel.add(lastRec - System.nanoTime());
      nxtPtrIdx = (int) search(((RefNode) curNode).templateRoot, sk, ofs + matLen);
      value.add(((RefNode) curNode).values[nxtPtrIdx]);
      return wrapLongLists(value, nanoLatencyByLevel, parLenByLevel, nodeTypeByLevel);
    }

    matLen = getMatchLength(curNode.getPartialKey(), sk, ofs);
    if (matLen == curNode.getPartialKey().length && matLen + ofs == sk.length) {
      nanoLatencyByLevel.add(System.nanoTime() - lastRec);
      value.add(curNode.getValue());
      return wrapLongLists(value, nanoLatencyByLevel, parLenByLevel, nodeTypeByLevel);
    }
    throw new RuntimeException("Key not exists: " + new String(sk, StandardCharsets.UTF_8));
  }

  @SafeVarargs
  private static long[][] wrapLongLists(List<Long>... lists) {
    long[][] res = new long[lists.length][];
    for (int i = 0; i < lists.length; i++) {
      res[i] = new long[lists[i].size()];
      for (int j = 0; j < res[i].length; j++) {
        res[i][j] = lists[i].get(j);
      }
    }
    return res;
  }

  public static List<ISEARTNode> getPrefixPaths(
      final ISEARTNode curNode, final byte[] sk, final int offset, List<ISEARTNode> _paths) {
    List<ISEARTNode> paths = _paths == null ? new ArrayList<>() : _paths;

    if (!curNode.isLeaf()) {
      int matLen = getMatchLength(curNode.getPartialKey(), sk, offset);
      if (matLen + offset == sk.length) {
        return paths;
      }
      int nxtPtrIdx = curNode.getPtrIdxByByte(sk[offset + matLen]);

      if (nxtPtrIdx >= 0) {
        paths.add(curNode);
        return getPrefixPaths(
            curNode.getChildByPtrIndex(nxtPtrIdx), sk, offset + matLen + 1, paths);
      }

      return null;
    }

    return paths;
  }

  public static long search(final ISEARTNode root, final byte[] sk, final int offset) {
    if (root == null) {
      throw new RuntimeException("Searching on null root.");
    }

    int ofs = offset, matLen, nxtPtrIdx;
    int[] res;
    ISEARTNode curNode = root;
    while (!curNode.isLeaf()) {
      matLen = getMatchLength(curNode.getPartialKey(), sk, ofs);
      nxtPtrIdx = curNode.getPtrIdxByByte(sk[ofs + matLen]);

      if (nxtPtrIdx >= 0) {
        ofs += matLen + 1;
        curNode = curNode.getChildByPtrIndex(nxtPtrIdx);
        continue;
      }

      throw new RuntimeException("Key not exists: " + new String(sk, StandardCharsets.UTF_8));
    }

    // todo faster check
    if (curNode instanceof RefNode) {
      matLen = getMatchLength(curNode.getPartialKey(), sk, ofs);

      if (matLen < curNode.getPartialKey().length) {
        throw new RuntimeException("Key not exists: " + new String(sk, StandardCharsets.UTF_8));
      }

      nxtPtrIdx = (int) search(((RefNode) curNode).templateRoot, sk, ofs + matLen);
      return ((RefNode) curNode).values[nxtPtrIdx];
    }

    matLen = getMatchLength(curNode.getPartialKey(), sk, ofs);
    if (matLen == curNode.getPartialKey().length && matLen + ofs == sk.length) {
      return curNode.getValue();
    }
    // todo add matched prefix and diverging byte in message
    throw new RuntimeException("Key not exists: " + new String(sk, StandardCharsets.UTF_8));
  }

  private static void checkKeysSorted(ISEARTNode node) {
    // todo only for debug
    if (node == null || node.getKeys() == null) return;

    byte[] keys = node.getKeys();
    for (int i = 0; i < keys.length - 1; i++) {
      if (keys[i] > keys[i + 1]) {
        System.out.println("WRONG.");
      }
    }
  }

  // region Observer

  public ISEARTNode displayPrefixDesc(String prefix) {
    return displayPrefixDesc(root, prefix);
  }

  public static ISEARTNode displayPrefixDesc(ISEARTNode root, String prefix) {
    byte[] kb = prefix.getBytes(StandardCharsets.UTF_8);
    Deque<ISEARTNode> trace = new ArrayDeque<>();
    int ofs = 0, cover = 0;
    ISEARTNode cur = root;

    while ((cover = getMatchLength(cur.getPartialKey(), kb, ofs)) != 0) {
      if (cover + ofs == kb.length) {
        break;
      }

      if (cover == cur.getPartialKey().length) {
        trace.addLast(cur);
        ofs += cover;
        cur = cur.getChildByKeyByte(kb[ofs]);
        ofs++;
      }
    }

    for (String dp : DFSTraversal.getAllPaths(cur)) {
      System.out.println(prefix + dp);
    }
    return cur;
  }

  // endregion

  public static void main(String[] args) {
    SEARTree tree = new SEARTree();
    tree.insert("root.sg1.d2.v2".getBytes(StandardCharsets.UTF_8), 1L);
    tree.insert("root.sg2.d3.v3".getBytes(StandardCharsets.UTF_8), 2L);
    tree.insert("root.sg2.d4.v1".getBytes(StandardCharsets.UTF_8), 3L);
    tree.insert("root.sg2.d3.v1".getBytes(StandardCharsets.UTF_8), 4L);
    tree.insert("root.sg2.xd3.xv1".getBytes(StandardCharsets.UTF_8), 11L);
    tree.insert("root.sg5.d1.v1".getBytes(StandardCharsets.UTF_8), 5L);
    tree.insert("root.sg5.d2.v1".getBytes(StandardCharsets.UTF_8), 6L);
    tree.insert("root.sg6.d1.v1".getBytes(StandardCharsets.UTF_8), 7L);

    tree.insert("root.sg8.d1.v1".getBytes(StandardCharsets.UTF_8), 7L);
    tree.insert("root.sg9.d1.v1".getBytes(StandardCharsets.UTF_8), 7L);
    tree.insert("root.sga.d1.v1".getBytes(StandardCharsets.UTF_8), 7L);
    tree.insert("root.sgb.d1.v1".getBytes(StandardCharsets.UTF_8), 7L);
    tree.insert("root.sgc.d1.v1".getBytes(StandardCharsets.UTF_8), 7L);
    tree.insert("root.sgd.d1.v1".getBytes(StandardCharsets.UTF_8), 7L);
    tree.insert("root.sge.d1.v1".getBytes(StandardCharsets.UTF_8), 7L);
    tree.insert("root.sgf.d1.v1".getBytes(StandardCharsets.UTF_8), 7L);
    tree.insert("root.sgg.d1.v1".getBytes(StandardCharsets.UTF_8), 7L);
    tree.insert("root.sgh.d1.v1".getBytes(StandardCharsets.UTF_8), 7L);
    tree.insert("root.sgi.d1.v1".getBytes(StandardCharsets.UTF_8), 7L);
    tree.insert("root.sgj.d1.v1".getBytes(StandardCharsets.UTF_8), 7L);
    tree.insert("root.sgk.d1.v1".getBytes(StandardCharsets.UTF_8), 7L);
    tree.insert("root.sgl.d1.v1".getBytes(StandardCharsets.UTF_8), 73121L);

    System.out.println("--------------");
    displayPrefixDesc(tree.root, "root.sg2");
  }
}
