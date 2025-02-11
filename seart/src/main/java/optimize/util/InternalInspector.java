package optimize.util;

import optimize.traversal.BoxPlotRecord;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class InternalInspector {
  static Map<String, Integer> statMap = new TreeMap<>(); // track Inner statistics
  static Map<String, List<Integer>> appendMap = new TreeMap<>();

  public static void incEntry(String key, int val) {
    statMap.compute(key, (k, v) -> v == null ? val : v + val);
  }

  public static void appendEntry(String key, int val) {
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

  public static void printResult() {
    StringBuilder builder = new StringBuilder();
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
    System.out.println(builder);
  }
}
