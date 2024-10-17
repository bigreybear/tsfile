package seart;

import seart.exception.PrefixPropertyException;

import java.nio.charset.StandardCharsets;

public class SEARTree {
  SEARTNode root;

  public SEARTree() {
    root = new Leaf(null, -1L);
  }

  /**
   * Compare with current Node (from root)
   * return match length and judge {
   *    ...
   *    curNode is not leaf but insKey exhausted: throw exception
   *
   *    pk exhausted {
   *      insKey finished: throw except
   *      else: add new key/ptr on curNode (may expand), pointing to a leaf
   *    }
   *
   *    pk not exhausted {
   *      break pk to pkp+pks
   *      pkp in 4 node, pointing to both curNode and a new Leaf
   *      pks in the new Leaf
   *    }
   * }
   * === above are drafts
   * Conform to Prefix Property (keys are not prefix to each other).
   * Which is consistent with MTree, i.e. no series path shall have subordinate nodes.
   */
  public void insert(byte[] insKey, long value) {
    SEARTNode curNode = root, parNode = null;
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
        // a new Node4 and a new Leaf

        splitPartialKey(parNode, parNodeIdx, insKey, curNode, res[0], ofs);
        return;
      } else {
        // res[0] == res[1]: pk exhausted, insKey not, and find no branching byte

        SEARTNode nl = new Leaf();
        curNode.insertWithExpand(parNode, parNodeIdx, insKey, ofs, res, nl);
        return;
        // if (curNode.isFull()) {
        //   migrate(parNode, insKey, curNode, res[0], ofs);
        // } else {
        //   // insertNewLeaf();
        //   SEARTNode nleaf = new Leaf();
        //   curNode.insert(insKey[ofs+res[0]], nleaf);
        // }
        // return;
      }
    }

    // determine: whether conform to prefix property
    // throw duplicate key
    //
    // incurs leaf, diverge on its partial key
  }

  private void splitPartialKey(SEARTNode parNode, int parNodeIdx, byte[] ik, SEARTNode cur, int overLen, int ofs) {

  }

  public static void main(String[] args) {
    Object[] a = new Object[10];
    for (Object i : a) {
      System.out.println(i);
    }
  }

}
