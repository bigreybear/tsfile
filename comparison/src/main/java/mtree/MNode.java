package mtree;

import org.apache.arrow.flatbuf.Null;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.lang.reflect.Field;

public class MNode {
  public static Field tableField;

  static {
    try {
      tableField = HashMap.class.getDeclaredField("table");
      tableField.setAccessible(true);
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
  }


  String name;
  Map<String, MNode> children;

  public MNode(String name) {
    this.name = name;
    this.children = new HashMap<>();
  }

  public boolean hasChild(String name) {
    return children.containsKey(name);
  }

  public MNode getChild(String name) {
    return children.getOrDefault(name, null);
  }

  public void addChild(MNode child) {
    if (hasChild(child.name)) {
      throw new RuntimeException("Duplicated children: " + child.name);
    }
    children.put(child.name, child);
  }

  public MNodeSpaceMetric getMetric() throws IllegalAccessException {
    List<MNodeSpaceMetric> childrenMetrics = new ArrayList<>();
    int tableLen = 0; // empty slots
    if (!children.isEmpty()) {
      Object[] table = (Object[]) tableField.get(children);
      tableLen = table.length;

      for (MNode node : children.values()) {
        childrenMetrics.add(node.getMetric());
      }
    }

    MNodeSpaceMetric childMetricSum = childrenMetrics.isEmpty()
        ? new MNodeSpaceMetric()
        : MNodeSpaceMetric.addsUp(childrenMetrics);

    MNodeSpaceMetric res = new MNodeSpaceMetric();
    res.inc(MNodeSpaceMetric.MetricType.CHILD_NAME_LEN, childMetricSum.get(MNodeSpaceMetric.MetricType.CHILD_NAME_LEN));
    res.inc(MNodeSpaceMetric.MetricType.CHILD_NAME_LEN, childMetricSum.get(MNodeSpaceMetric.MetricType.NAME_LEN));

    res.inc(MNodeSpaceMetric.MetricType.NAME_LEN, name.length());
    res.inc(MNodeSpaceMetric.MetricType.EMBEDDED_SPACE, 16); // 2 field pointer

    res.inc(MNodeSpaceMetric.MetricType.EMPT_SLT, tableLen - children.size());
    res.inc(MNodeSpaceMetric.MetricType.EMPT_SLT, childMetricSum.get(MNodeSpaceMetric.MetricType.EMPT_SLT));

    res.set(MNodeSpaceMetric.MetricType.NODE_CNT, childMetricSum.get(MNodeSpaceMetric.MetricType.NODE_CNT));
    res.inc(MNodeSpaceMetric.MetricType.NODE_CNT, 1);
    return res;
  }
}
