package optimize.merge;

import optimize.TSTree;
import optimize.nodes.INode;
import optimize.nodes.fdm.FLeaf;
import optimize.nodes.fdm.IFNode;
import optimize.nodes.logic.LLeaf;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

import static optimize.util.ByteArray.concatenate;

public class MergedTreeTraversal {

  public static void FDMMergeTraverse(
      IFNode par,
      Deque<byte[]> trace,
      IFNode cur,
      byte key,
      TSTree.IQuadFunction<IFNode, Byte, IFNode, Deque<byte[]>> consumer) {
    if (trace == null) trace = new ArrayDeque<>();

    if (cur instanceof LLeaf || cur.getFValue() instanceof LLeaf) {
      consumer.apply(par, (byte)0, cur, trace);
      return;
    }

    if (cur instanceof FLeaf) {
      FDMMergeTraverse(cur, trace, (IFNode) cur.getFValue(), (byte) 0, consumer);
      // res = cur.getFValue();
      // if (res instanceof LLeaf) return;
      // cur = (IFNode) res;
    }

    byte[] keys = cur.getKeysFromFDM();

    if (keys == null || keys.length == 0) {
      return;
    }

    for (byte k : keys) {
      byte[] token = concatenate(cur.getPartialKey() == null ? new byte[0] : cur.getPartialKey(), k);
      trace.addLast(token);
      FDMMergeTraverse(cur, trace, (IFNode) cur.get(k), k, consumer);
      trace.removeLast();
    }

    consumer.apply(par, key, cur, trace);
  }

  public static void HashMergeTraverse(
      INode par,
      Deque<byte[]> trace,
      INode cur,
      byte[] key,
      TSTree.IQuadFunction<INode, byte[], INode, Deque<byte[]>> consumer) {
    if (trace == null) trace = new ArrayDeque<>();

    List<byte[]> keys = cur.getKeyBytes();
    if (keys == null || keys.isEmpty()) {
      consumer.apply(par, key, cur, trace);
      return;
    }

    for (byte[] k : keys) {
      byte[] token = concatenate(k, cur.getPartialKey() == null ? new byte[0] : cur.getPartialKey());

      trace.addLast(token);
      HashMergeTraverse(cur, trace, cur.getChildByBytes(k), k, consumer);
      trace.removeLast();
    }

    consumer.apply(par, key, cur, trace);
  }
}
