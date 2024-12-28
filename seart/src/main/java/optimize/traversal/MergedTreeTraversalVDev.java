package optimize.traversal;

import static optimize.util.ByteArray.concatenate;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import optimize.TSTreeVDev;
import optimize.nodes.IMicroNode;
import optimize.nodes.cdm.CLeaf;
import optimize.nodes.cdm.ICNode;
import optimize.nodes.fdm.FLeaf;
import optimize.nodes.fdm.IFNode;
import optimize.nodes.logic.LLeaf;

public class MergedTreeTraversalVDev {

  public static void CDMMergeTraverse(
      ICNode par,
      Deque<byte[]> trace,
      ICNode cur,
      byte[] key,
      TSTreeVDev.IQuadFunction<ICNode, byte[], ICNode, Deque<byte[]>> consumer) {
    if (trace == null) trace = new ArrayDeque<>();

    if (cur instanceof LLeaf) {
      consumer.apply(par, null, cur, trace);
      return;
    }

    if (cur instanceof CLeaf && (((CLeaf) cur).ptr instanceof LLeaf)) {
      consumer.apply(par, null, cur, trace);
      return;
    }

    if (cur instanceof CLeaf) {
      // pointing to an internal node
      CDMMergeTraverse(cur, trace, (ICNode) ((CLeaf) cur).ptr, null, consumer);
      return;
    }

    byte[][] keys = cur.getBranchingKeys();
    if (keys == null || keys.length == 0) return;

    for (byte[] k : keys) {
      if (cur.getChild(k) instanceof LLeaf) continue;

      byte[] token = concatenate(cur.getParKey() == null ? new byte[0] : cur.getParKey(), k);
      trace.addLast(token);
      CDMMergeTraverse(cur, trace, (ICNode) cur.getChild(k), k, consumer);
      trace.removeLast();
    }

    consumer.apply(par, key, cur, trace);
  }

  public static void FDMMergeTraverse(
      IFNode par,
      Deque<byte[]> trace,
      IFNode cur,
      byte key,
      TSTreeVDev.IQuadFunction<IFNode, Byte, IFNode, Deque<byte[]>> consumer) {
    if (trace == null) trace = new ArrayDeque<>();

    if (cur instanceof LLeaf || cur.getFValue() instanceof LLeaf) {
      consumer.apply(par, (byte) 0, cur, trace);
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
      byte[] token = concatenate(cur.getParKey() == null ? new byte[0] : cur.getParKey(), k);
      trace.addLast(token);
      FDMMergeTraverse(cur, trace, (IFNode) cur.get(k), k, consumer);
      trace.removeLast();
    }

    consumer.apply(par, key, cur, trace);
  }

  public static void HashMergeTraverse(
      IMicroNode par,
      Deque<byte[]> trace,
      IMicroNode cur,
      byte[] key,
      TSTreeVDev.IQuadFunction<IMicroNode, byte[], IMicroNode, Deque<byte[]>> consumer) {
    if (trace == null) trace = new ArrayDeque<>();

    List<byte[]> keys = cur.getKeyBytes();
    if (keys == null || keys.isEmpty()) {
      consumer.apply(par, key, cur, trace);
      return;
    }

    for (byte[] k : keys) {
      byte[] token = concatenate(k, cur.getParKey() == null ? new byte[0] : cur.getParKey());

      trace.addLast(token);
      HashMergeTraverse(cur, trace, cur.getChild(k), k, consumer);
      trace.removeLast();
    }

    consumer.apply(par, key, cur, trace);
  }
}
