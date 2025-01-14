package optimize.nodes;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import optimize.ExpResultLogger;
import optimize.nodes.logic.LLeaf;
import optimize.traversal.BoxPlotRecord;

public class NodeInspector {
  int curDepth = 1;
  ArrayList<Integer> depthRecs = new ArrayList<>();
  Map<String, Integer> statMap = new TreeMap<>(); // track Inner statistics
  Map<String, List<Integer>> appendMap = new TreeMap<>();
  Deque<ITSNode> nodeStk = new ArrayDeque<>(); // support traversal
  ITSNode PAD_MARK = new LLeaf(-1); // mark for deeper level

  public int getCurDepth() {
    return curDepth;
  }

  public void incEntry(String key, int val) {
    statMap.compute(key, (k, v) -> v == null ? val : v + val);
  }

  public void appendEntry(String key, int val) {
    appendMap.compute(
        key,
        (k, v) -> {
          if (v == null) {
            v = new ArrayList<>();
          }
          v.add(val);
          return v;
        });
  }

  public void inspect(final ITSNode node) {

    ITSNode cur = node;
    nodeStk.addLast(cur);
    while (!nodeStk.isEmpty()) {
      cur = nodeStk.removeLast();

      if (cur == PAD_MARK) {
        curDepth--;
        continue;
      }

      if (cur.isLogicalLeaf()) {
        cur.acceptInspector(this);
        depthRecs.add(curDepth);
        continue;
      }

      cur.acceptInspector(this);
      nodeStk.addLast(PAD_MARK);
      curDepth++;
      nodeStk.addAll(cur.getPhysicalChildren());
    }
  }

  public void dumpDepthResults(ExpResultLogger logger) {
    logger.recordDepth(BoxPlotRecord.calculateBoxPlot(depthRecs));
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("Depth:");
    builder.append(BoxPlotRecord.calculateBoxPlot(depthRecs));
    builder.append("\n");
    for (Map.Entry<String, Integer> entry : statMap.entrySet()) {
      builder.append(String.format("%s: %d\n", entry.getKey(), entry.getValue()));
    }

    for (Map.Entry<String, List<Integer>> entry : appendMap.entrySet()) {
      int s = entry.getValue().stream().mapToInt(Integer::intValue).sum();
      builder.append(
          String.format(
              "%s: -sum=%d -avg=%d -dist=%s\n",
              entry.getKey(),
              s,
              s / entry.getValue().size(),
              entry.getValue().size() < 5
                  ? String.format(
                      "(val: %s)", Arrays.toString(entry.getValue().toArray(new Integer[0])))
                  : BoxPlotRecord.calculateBoxPlot(entry.getValue())));
    }
    return builder.toString();
  }
}
