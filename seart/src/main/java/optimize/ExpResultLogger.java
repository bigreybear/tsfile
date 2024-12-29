package optimize;

import static optimize.MainSupport.dottedNanoSec;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import optimize.merge.MapType;
import optimize.merge.PrefixMergeStrategy;
import optimize.traversal.BoxPlotRecord;

public class ExpResultLogger {
  public static final String RESULT_SPACE = "Exp_Space.txt";
  public static final String RESULT_LATENCY = "Exp_Latency.txt";
  public static final String RESULT_DEPTH = "Exp_Depth.txt";

  public long space = -1L;
  public long latency = -1L;
  public MyDataSet mds;
  public PrefixMergeStrategy pms;
  public MapType mapType;
  public boolean twoLevel;

  public static boolean checkEmptyFile(String path) {
    File f = new File(path);
    if (!f.exists()) {
      try {
        f.createNewFile();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    return f.length() == 0;
  }

  public void recordSpace() {
    boolean emptyFile = checkEmptyFile(RESULT_SPACE);

    try (BufferedWriter writer = new BufferedWriter(new FileWriter(RESULT_SPACE, true))) {
      if (emptyFile) {
        writer.write("Stg\tDts\tMpt\tTwl\tSpc");
        writer.newLine();
      }

      writer.write(
          String.format(
              "%s\t%s\t%s\t%s\t%s", pms.name(), mds.name(), mapType.name(), twoLevel, space));
      writer.newLine();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void recordLatency() {
    boolean emptyFile = checkEmptyFile(RESULT_LATENCY);
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(RESULT_LATENCY, true))) {
      if (emptyFile) {
        writer.write("Stg\tDts\tMpt\tTwl\tLat");
        writer.newLine();
      }

      writer.write(
          String.format(
              "%s\t%s\t%s\t%s\t%s",
              pms.name(), mds.name(), mapType.name(), twoLevel, dottedNanoSec(latency)));
      writer.newLine();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void recordDepth(BoxPlotRecord bpr) {
    boolean emptyFile = checkEmptyFile(RESULT_DEPTH);
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(RESULT_DEPTH, true))) {
      if (emptyFile) {
        writer.write("Stg\tDts\tMpt\tTwl\tmin\tq1\tmed\tq3\tmax");
        writer.newLine();
      }

      writer.write(
          String.format(
              "%s\t%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d",
              pms.name(), mds.name(), mapType.name(), twoLevel,
              bpr.min, bpr.q1, bpr.median, bpr.q3, bpr.max));
      writer.newLine();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
