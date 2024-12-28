package optimize.traversal;

import static optimize.util.ByteArray.concatenate;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import optimize.merge.MapType;
import optimize.nodes.IMicroNode;
import optimize.nodes.ITSNode;
import optimize.nodes.cdm.CLeaf;
import optimize.nodes.cdm.ICNode;
import optimize.nodes.fdm.FLeaf;
import optimize.nodes.fdm.IFNode;
import optimize.nodes.logic.LLeaf;

public class MergedTreeTraversalForDepthVDev {

  public static final List<Integer> depthList = new ArrayList<>();

  public static BoxPlotRecord calculateBoxPlot(List<Integer> data) {
    if (data == null || data.isEmpty()) {
      throw new IllegalArgumentException("Data list cannot be null or empty");
    }

    BoxPlotRecord record = new BoxPlotRecord();

    List<Integer> res = new ArrayList<>();
    Collections.sort(data);
    record.min = data.get(0);
    record.max = data.get(data.size() - 1);

    record.median = getMedian(data);
    record.q1 = getMedian(data.subList(0, data.size() / 2));
    record.q3 = getMedian(data.subList((data.size() + 1) / 2, data.size()));
    record.iqr = record.q3 - record.q1;

    int lowerBound = (int) (record.q1 - 1.5 * record.iqr);
    int upperBound = (int) (record.q3 + 1.5 * record.iqr);

    for (int num : data) {
      if (num < lowerBound || num > upperBound) {
        System.out.println(num);
        record.outliers.add(num);
      }
    }
    return record;
  }

  private static int getMedian(List<Integer> data) {
    int size = data.size();
    if (size % 2 == 0) {
      return (int) ((data.get(size / 2 - 1) + data.get(size / 2)) / 2.0);
    } else {
      return data.get(size / 2);
    }
  }

  public static void CDMTraverseForDepth(ICNode par, Deque<byte[]> trace, ICNode cur, int depth) {
    if (trace == null) trace = new ArrayDeque<>();

    if (cur instanceof LLeaf) {
      depthList.add(depth);
      return;
    }

    if (cur instanceof CLeaf && (((CLeaf) cur).ptr instanceof LLeaf)) {
      // just a fast return
      depthList.add(depth + 1);
      return;
    }

    if (cur instanceof CLeaf) {
      // pointing to an internal node
      CDMTraverseForDepth(cur, trace, (ICNode) ((CLeaf) cur).ptr, depth + 1);
      return;
    }

    byte[][] keys = cur.getBranchingKeys();

    for (byte[] k : keys) {
      if (cur.getChild(k) instanceof LLeaf) continue;

      byte[] token = concatenate(cur.getParKey() == null ? new byte[0] : cur.getParKey(), k);
      trace.addLast(token);
      CDMTraverseForDepth(cur, trace, (ICNode) cur.getChild(k), depth + 1);
      trace.removeLast();
    }
  }

  public static void FDMTraverseForDepth(IFNode par, Deque<byte[]> trace, IFNode cur, int depth) {
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
      byte[] token = concatenate(cur.getParKey() == null ? new byte[0] : cur.getParKey(), k);
      trace.addLast(token);
      FDMTraverseForDepth(cur, trace, (IFNode) cur.get(k), depth + 1);
      trace.removeLast();
    }
  }

  public static void HashTraverseForDepth(
      IMicroNode par, Deque<byte[]> trace, IMicroNode cur, int depth) {
    if (trace == null) trace = new ArrayDeque<>();

    List<byte[]> keys = cur.getKeyBytes();
    if (keys == null || keys.isEmpty()) {
      depthList.add(depth);
      return;
    }

    for (byte[] k : keys) {
      byte[] token = concatenate(k, cur.getParKey() == null ? new byte[0] : cur.getParKey());

      trace.addLast(token);
      HashTraverseForDepth(cur, trace, cur.getChild(k), depth + 1);
      trace.removeLast();
    }
  }

  public static BoxPlotRecord collectDepths(ITSNode root, MapType mt) {
    switch (mt) {
      case CDM:
        CDMTraverseForDepth(null, null, (ICNode) root, 0);
        break;
      case FDM:
        FDMTraverseForDepth(null, null, (IFNode) root, 0);
        break;
      case HASH:
        HashTraverseForDepth(null, null, (IMicroNode) root, 0);
        break;
      default:
        throw new UnsupportedOperationException();
    }
    return calculateBoxPlot(depthList);
  }
}
