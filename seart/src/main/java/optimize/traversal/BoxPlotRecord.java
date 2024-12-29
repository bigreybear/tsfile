package optimize.traversal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BoxPlotRecord {
  public int min, q1, median, q3, max, iqr;
  public List<Integer> outliers = new ArrayList<>();

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
        record.outliers.add(num);
      }
    }
    return record;
  }

  private static int getMedian(List<Integer> data) {
    int size = data.size();

    if (data.isEmpty()) return 0;
    else if (data.size() == 1) return data.get(0);
    else if (data.size() == 2) return (int) ((data.get(0) + data.get(1))/2.0);

    if (size % 2 == 0) {
      return (int) ((data.get(size / 2 - 1) + data.get(size / 2)) / 2.0);
    } else {
      return data.get(size / 2);
    }
  }

  @Override
  public String toString() {
    return String.format(
        "(min, q1, med, q3, max: %d, %d, %d, %d, %d)", min, q1, median, q3, max);
  }
}
