package optimize.nodes.fdm.vfull;

import loader.PathTxtLoader;
import optimize.nodes.INode;
import org.openjdk.jol.info.GraphLayout;
import seart.exception.PrefixPropertyException;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static seart.ISEARTNode.getMatchLength;

public class SEARTree implements SeriesIndexTree, Serializable {
  public ISEARTNode root;

  public SEARTree() {}

  public SEARTree(ISEARTNode root) {
    this.root = root;
  }

  @Override
  public void insert(String key, INode value) {
    insert(key.getBytes(StandardCharsets.UTF_8), value);
  }

  public void insert(byte[] insKey, INode value) {
    root = insert(root, insKey, value);
  }

  // return the new root
  public static ISEARTNode insert(ISEARTNode root, byte[] insKey, INode value) {
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
      ISEARTNode expNode = curNode.insert(insKey[ofs+matLen], nxtPtrIdx, nl);

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
      byte[] ik, ISEARTNode cur, int ofs, int overLen, INode value) {
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


  public static int nullKeys = 0;
  @Override
  public INode search(String sk) {
    try {
      return search(sk.getBytes(StandardCharsets.UTF_8));
    } catch (RuntimeException e) {
      System.out.println("key not exist");
      nullKeys++;
      return null;
    }
  }

  public INode search(byte[] sk) {
    return search(this.root, sk, 0);
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

  public static INode search(final ISEARTNode root, final byte[] sk, final int offset) {
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
    // if (curNode instanceof RefNode) {
    //   matLen = getMatchLength(curNode.getPartialKey(), sk, ofs);
    //
    //   if (matLen < curNode.getPartialKey().length) {
    //     throw new RuntimeException("Key not exists: " + new String(sk, StandardCharsets.UTF_8));
    //   }
    //
    //   nxtPtrIdx = (int) search(((RefNode) curNode).templateRoot, sk, ofs + matLen);
    //   return ((RefNode) curNode).values[nxtPtrIdx];
    // }

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

  public static void main(String[] args) {
    SEARTree tree = new SEARTree();
    try (PathTxtLoader loader = new PathTxtLoader(PathTxtLoader.FILE_PATH)) {
      List<String> paths = loader.getAllLines();
      for (String s : paths) {
        try {
          tree.insert(s, null);
        } catch (PrefixPropertyException e) {
          e.printStackTrace();
          System.out.println(s);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    System.out.println(GraphLayout.parseInstance(tree).totalSize());
  }
}
