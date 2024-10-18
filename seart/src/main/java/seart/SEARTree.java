package seart;

import seart.exception.PrefixPropertyException;
import seart.traversal.DFSTraversal;
import seart.traversal.Traverser;
import sun.util.resources.cldr.sk.TimeZoneNames_sk;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;

public class SEARTree {
  public ISEARTNode root;

  public SEARTree() {
  }

  public void insert(String key, long value) {
    insert(key.getBytes(StandardCharsets.UTF_8), value);
  }

  public void insert(byte[] inskey, long value) {
    root = insert(root, inskey, value);
  }

  // return the new root
  public static ISEARTNode insert(final ISEARTNode root, byte[] insKey, long value) {
    if (root == null) {
      return new Leaf(insKey, value);
    }

    ISEARTNode curNode = root, parNode = null;
    int parNodeIdx = -1;  // index of array within parent node

    // ofs for offset processing on the insert key
    int ofs = 0;
    int[] res;
    byte divByte;
    while (!curNode.isLeaf()) {
      // MAGIC ARRAY: res[overlapped len, part key len, branching key idx]
      res = curNode.matchPartialKey(insKey, ofs);

      if (res[2] >= 0) {
        ofs += res[0] + 1; // plus one for the branching byte
        parNode = curNode;
        parNodeIdx = res[2];
        curNode = curNode.getChildByPtrIndex(res[2]);
        continue;
      }

      if (res[0] + ofs == insKey.length) {
        throw new PrefixPropertyException(insKey);
      }

      if (res[0] < res[1]) {
        // pk not exhausted, but partially matched
        // new a Node4 and a Leaf
        return updateRoot(
            root,
            parNode,
            parNodeIdx,
            splitPartialKey(insKey, curNode, ofs, res[0], value));
      } else {
        // res[0] == res[1]: pk exhausted, insKey not, and find NO branching byte
        ISEARTNode nl = new Leaf(Arrays.copyOfRange(insKey, ofs + res[0] + 1, insKey.length), value);
        ISEARTNode expNode = curNode.insertWithExpand(insKey, ofs, res, nl);

        if (expNode == null) {
          return root;
        }

        if (parNode == null) {
          return expNode;
        }

        parNode.setChildPtrByIndex(parNodeIdx, expNode);
        return root;
      }
    }

    res = curNode.matchPartialKey(insKey, ofs);
    if (res[0] < res[1]) {
      // split partial key
      return updateRoot(
          root,
          parNode,
          parNodeIdx,
          splitPartialKey(insKey, curNode, ofs, res[0], value));
    }
    throw new PrefixPropertyException(insKey);
  }

  // serves insert process
  private static SEARTNode splitPartialKey(
      byte[] ik,
      ISEARTNode cur, int ofs, int overLen, long value) {
    // from ofs+overLen+1 for 1 byte as branching key in the new Node4
    Leaf leaf = new Leaf(Arrays.copyOfRange(ik, ofs + overLen + 1, ik.length), value);
    SEARTNode n4 = new Node4(
        Arrays.copyOfRange(ik, ofs, ofs + overLen),
        cur.getPartialKey()[overLen], cur,
        ik[ofs + overLen], leaf
    );
    cur.reassignPartialKey(
        Arrays.copyOfRange(cur.getPartialKey(), overLen + 1, cur.getPartialKey().length)
    );
    return n4;
  }

  private static ISEARTNode updateRoot(final ISEARTNode root, ISEARTNode par, int parIdx, SEARTNode n4) {
    if (par != null) {
      par.setChildPtrByIndex(parIdx, n4);
      return root;
    }
    return n4;
  }

  public long search(String sk) {
    return search(sk.getBytes(StandardCharsets.UTF_8));
  }

  public long search(byte[] sk) {
    return search(this.root, sk, 0);
  }

  public static long search(final ISEARTNode root, final byte[] sk, final int offset) {
    if (root == null) {
      throw new RuntimeException("Searching on null root.");
    }

    int ofs = offset;
    int[] res;
    ISEARTNode curNode = root;
    while (!curNode.isLeaf()) {
      res = curNode.matchPartialKey(sk, ofs);

      if (res[2] >= 0) {
        ofs += res[0] + 1;
        curNode = curNode.getChildByPtrIndex(res[2]);
        continue;
      }

      throw new RuntimeException("Key not exists");
    }

    // todo faster check
    if (curNode instanceof RefNode) {
      res = curNode.matchPartialKey(sk, ofs);
      if (res[0] != 1) {
        throw new RuntimeException("Key not exists: " + new String(sk, StandardCharsets.UTF_8));
      }
      return ((RefNode)curNode).values[res[1]];
    }

    res = curNode.matchPartialKey(sk, ofs);
    if (res[0] == res[1] && res[0] + ofs == sk.length) {
      return curNode.getValue();
    }
    throw new RuntimeException("Key not exists: " + new String(sk, StandardCharsets.UTF_8));
  }



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

    // tree.insert("root.sg8.d1.v12".getBytes(StandardCharsets.UTF_8), 7L);
    System.out.println(tree);

    System.out.println(tree.search("root.sgl.d1.v1"));
    // Traverser.traverseDFS(tree.root, null);

    DFSTraversal dfsTraversal = new DFSTraversal(tree.root);
    ISEARTNode node;
    while (dfsTraversal.hasNext()) {
      node = dfsTraversal.next();
      if (node.isLeaf()) {
        // System.out.println(dfsTraversal.getCurrentPath());
      }

      if (node instanceof Node4) {
        System.out.println(dfsTraversal.getCurrentPath());
      }
    }
  }

}
