package mtree;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MNodeSpaceMetric {

  public enum MetricType {
    EMBEDDED_SPACE,
    NAME_LEN,
    CHILD_NAME_LEN,
    EMPT_SLT,
    NODE_CNT;
  }

  Map<String, Integer> counter = new HashMap<>();

  public void set(MetricType type, int val) {
    counter.put(type.name(), val);
  }

  public void inc(MetricType type, int val) {
    counter.put(type.name(), counter.getOrDefault(type.name(), 0) + val);
  }

  public int get(MetricType type) {
    return counter.getOrDefault(type.name(), 0);
  }

  public static MNodeSpaceMetric addsUp(List<MNodeSpaceMetric> metrics) {
    MNodeSpaceMetric res = new MNodeSpaceMetric();
    for (MNodeSpaceMetric metric : metrics) {
      for (MetricType type : MetricType.values()) {
        res.set(type, res.get(type) + metric.get(type));
      }
    }
    return res;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    for (MetricType type : MetricType.values()) {
      builder.append(String.format("%s:%d,", type.name(), counter.getOrDefault(type.name(), 0)));
    }

    return builder.toString();
  }

}
