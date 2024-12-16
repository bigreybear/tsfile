package optimize.traversal;

import java.util.ArrayList;
import java.util.List;

public class BoxPlotRecord {
  public int min, q1, median, q3, max, iqr;
  public List<Integer> outliers = new ArrayList<>();

  @Override
  public String toString() {
    return String.format("(min, q1, med, q3, max, iqr: %d, %d, %d, %d, %d)",
        min, q1, median, q3, max);
  }
}
