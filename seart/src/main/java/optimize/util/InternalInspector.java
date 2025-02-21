package optimize.util;

import optimize.traversal.BoxPlotRecord;
import optimize.traversal.BoxPlotRecordLong;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

// for those metrics significantly affects performance,
// they shall be measured in extra runs with this.
public class InternalInspector {
  static Map<String, Integer> statMap = new TreeMap<>(); // track Inner statistics
  static Map<String, List<Integer>> appendMap = new TreeMap<>();
  static Map<String, List<Long>> appendMapLong = new TreeMap<>();

  public static void reset() {
    statMap = new TreeMap<>(); // track Inner statistics
    appendMap = new TreeMap<>();
    appendMapLong = new TreeMap<>();
  }

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

  public static void appendEntry(String key, long val) {
    appendMapLong.compute(
        key,
        (k, v) -> {
          if (v == null) {
            v = new ArrayList<>();
          }
          v.add(val);
          return v;
        });
  }

  public static void printResultOnConsole() {
    StringBuilder builder = new StringBuilder();
    for (Map.Entry<String, Integer> entry : statMap.entrySet()) {
      builder.append(String.format("%s: %d\n", entry.getKey(), entry.getValue()));
    }

    for (Map.Entry<String, List<Integer>> entry : appendMap.entrySet()) {
      int s = entry.getValue().stream().mapToInt(Integer::intValue).sum();
      builder.append(
          String.format(
              "%s: -num:%d -sum=%d -avg=%d -dist=%s\n",
              entry.getKey(),
              entry.getValue().size(),
              s,
              s / entry.getValue().size(),
              entry.getValue().size() < 5
                  ? String.format(
                  "(val: %s)", Arrays.toString(entry.getValue().toArray(new Integer[0])))
                  : BoxPlotRecord.calculateBoxPlot(entry.getValue())));
    }

    for (Map.Entry<String, List<Long>> entry : appendMapLong.entrySet()) {
      long s = entry.getValue().stream().mapToLong(Long::longValue).sum();
      builder.append(
          String.format(
              "%s: -num:%d -sum=%d -avg=%d -dist=%s\n",
              entry.getKey(),
              entry.getValue().size(),
              s,
              s / entry.getValue().size(),
              entry.getValue().size() < 5
                  ? String.format(
                  "(val: %s)", Arrays.toString(entry.getValue().toArray(new Long[0])))
                  : BoxPlotRecordLong.calculateBoxPlot(entry.getValue())));
    }
    System.out.println(builder);
  }
}
