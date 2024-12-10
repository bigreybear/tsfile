package optimize.traversal;

import optimize.nodes.INode;
import optimize.nodes.cdm.CLeaf;
import optimize.nodes.cdm.ICNode;
import optimize.nodes.fdm.FLeaf;
import optimize.nodes.fdm.IFNode;
import optimize.nodes.logic.LLeaf;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import static optimize.util.ByteArray.concatenate;

public class MergedTreeTraversalForDepth {

  public static final List<Integer> depthList = new ArrayList<>();

  public static void CDMTraverseForDepth(
      ICNode par,
      Deque<byte[]> trace,
      ICNode cur, int depth) {
    if (trace == null) trace = new ArrayDeque<>();

    if (cur instanceof LLeaf) {
      depthList.add(depth);
      return;
    }

    if (cur instanceof CLeaf && (((CLeaf) cur).ptr instanceof LLeaf)) {
      // just a fast return
      depthList.add(depth+1);
      return;
    }

    if (cur instanceof CLeaf) {
      // pointing to an internal node
      CDMTraverseForDepth(cur, trace, (ICNode) ((CLeaf) cur).ptr, depth+1);
      return;
    }

    byte[][] keys = cur.getKeysFromCDM();

    for (byte[] k : keys) {
      if (cur.getChildByBytes(k) instanceof LLeaf) continue;

      byte[] token = concatenate(cur.getPartialKey() == null ? new byte[0] : cur.getPartialKey(), k);
      trace.addLast(token);
      CDMTraverseForDepth(cur, trace, (ICNode) cur.getChildByBytes(k), depth+1);
      trace.removeLast();
    }

  }

  public static void FDMTraverseForDepth(
      IFNode par,
      Deque<byte[]> trace,
      IFNode cur, int depth) {
    if (trace == null) trace = new ArrayDeque<>();

    if (cur instanceof LLeaf) {
      depthList.add(depth);
      return;
    }

    if (cur.getFValue() instanceof LLeaf) {
      depthList.add(depth + 1);
      return;
    }

    if (cur instanceof FLeaf) {
      FDMTraverseForDepth(cur, trace, (IFNode) cur.getFValue(), depth + 1);
    }

    byte[] keys = cur.getKeysFromFDM();

    if (keys == null || keys.length == 0) {
      return;
    }

    for (byte k : keys) {
      byte[] token = concatenate(cur.getPartialKey() == null ? new byte[0] : cur.getPartialKey(), k);
      trace.addLast(token);
      FDMTraverseForDepth(cur, trace, (IFNode) cur.get(k), depth  +1);
      trace.removeLast();
    }
  }

  public static void HashTraverseForDepth(
      INode par,
      Deque<byte[]> trace,
      INode cur, int depth) {
    if (trace == null) trace = new ArrayDeque<>();

    List<byte[]> keys = cur.getKeyBytes();
    if (keys == null || keys.isEmpty()) {
      depthList.add(depth);
      return;
    }

    for (byte[] k : keys) {
      byte[] token = concatenate(k, cur.getPartialKey() == null ? new byte[0] : cur.getPartialKey());

      trace.addLast(token);
      HashTraverseForDepth(cur, trace, cur.getChildByBytes(k), depth + 1);
      trace.removeLast();
    }
  }
}
