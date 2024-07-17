package org.apache.tsfile.exps;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.tsfile.exps.conf.MergedDataSets;
import org.apache.tsfile.exps.loader.legacy.GeoLifeLoader;
import org.apache.tsfile.exps.loader.legacy.REDDLoader;
import org.apache.tsfile.exps.loader.legacy.TDriveLoader;
import org.apache.tsfile.exps.loader.legacy.TSBSLoader;
import org.apache.tsfile.exps.updated.LoaderBase;
import org.apache.tsfile.exps.utils.ProgressReporter;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;

public class ConditionGeneratorV2 implements Serializable {

  private static final long serialVersionUID = 3267199406246928943L;

  // how many conditions per type
  public static int CONDITION_CARD = 100;
  // how many points per range
  public static int RANGE_SPAN = 30;

  public abstract class CompCondition implements Serializable {
    public String series;

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      CompCondition a = (CompCondition) o;
      return series.equals(a.series);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(series);
    }
  }

  public class TimeRange extends CompCondition implements Serializable{
    public long t1, t2;
    public TimeRange(String s, long t1, long t2) {
      this.series = s;
      this.t1 = t1;
      this.t2 = t2;
    }
  }

  public class DoubleRange extends CompCondition implements Serializable {
    public double v1, v2;
    public DoubleRange(String s, double d1, double d2) {
      series = s;
      v1 = Math.min(d1, d2);
      v2 = Math.max(d1, d2);;
    }
  }

  public class MixedRange extends CompCondition implements Serializable {
    public double v1, v2;
    public long t1, t2;
    public MixedRange(String s, long t1, long t2, double d1, double d2) {
      series = s;
      this.t1 = t1;
      this.t2 = t2;
      v1 = Math.min(d1, d2);
      v2 = Math.max(d1, d2);
    }
  }

  public class CrossRange extends CompCondition implements Serializable {
    public String cSeries; // c for crossed, inherited series to be filtered
    public double v1, v2;
    public CrossRange(String filtered, String crossSelected, double d1, double d2) {
      series = filtered; cSeries = crossSelected;
      v1 = Math.min(d1, d2);
      v2 = Math.max(d1, d2);
    }
  }

  public Set<String> singleSeries = new HashSet<>();
  public Set<String> alignedDevices = new HashSet<>();
  public Set<TimeRange> timeRanges = new HashSet<>();
  public Set<DoubleRange> doubleRanges = new HashSet<>();
  public Set<MixedRange> mixedRanges = new HashSet<>();
  public Set<CrossRange> crossRanges = new HashSet<>();

  public ConditionGeneratorV2() {}

  public static void serialize(ConditionGeneratorV2 cg, String path) throws IOException {
    FileOutputStream fos = new FileOutputStream(path);
    ObjectOutputStream oos = new ObjectOutputStream(fos);

    oos.writeObject(cg);
    oos.close();
  }

  public static ConditionGeneratorV2 deserialize(String path) throws IOException, ClassNotFoundException {
    try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(path))){
      return (ConditionGeneratorV2) ois.readObject();
    }
  }


  public static ConditionGeneratorV2 getConditionsByDataSets(DataSets dataset) throws IOException, ClassNotFoundException {
    return ConditionGeneratorV2.deserialize(dataset.getConditionBinPath());
  }

  public static ConditionGeneratorV2 getConditionsByDataSets(String path) throws IOException, ClassNotFoundException {
    return ConditionGeneratorV2.deserialize(path);
  }

  private void sampleDoubleRangesNew(LoaderBase loaderBase) throws IOException {
    loaderBase.initIterator();
    List<RowIndexRange> res = getTargetRowRange(loaderBase);

    for (RowIndexRange rir : res) {
      String dev = loaderBase.getIDString((int) rir.start);
      FieldVector vec = loaderBase.getVector(rir.vecName);

      double str = 0.0, end = 0.0;
      switch (vec.getMinorType()) {
        case FLOAT8:
          Float8Vector f8v = (Float8Vector) vec;
          str = f8v.get(loaderBase.reloadAndGetLocalIndex(rir.valStr));
          end = f8v.get(loaderBase.reloadAndGetLocalIndex(rir.valEnd));
          break;
        case FLOAT4:
          Float4Vector f4v = (Float4Vector) vec;
          str = f4v.get(loaderBase.reloadAndGetLocalIndex(rir.valStr));
          end = f4v.get(loaderBase.reloadAndGetLocalIndex(rir.valEnd));
          break;
        default:
          System.out.println("Wrong type for double filtering.");
          System.exit(-1);
      }

      if (str == end) {
        continue;
      }
      doubleRanges.add(new DoubleRange(dev + "." + rir.vecName, str, end));
    }

    System.out.println(String.format("Finish generate %d Double Ranges.", doubleRanges.size()));
  }

  private void sampleAlignedNew(LoaderBase loaderBase) throws IOException {
    loaderBase.initIterator();
    List<RowIndexRange> res = getTargetRowRange(loaderBase);

    Set<String> uniDev = new HashSet<>();
    for (RowIndexRange rir : res) {
      uniDev.add(loaderBase.getIDString((int) rir.start));
    }

    System.out.println(String.format("Record %d devices for aligned query.", uniDev.size()));
    alignedDevices.addAll(uniDev);
  }

  private void sampleSeriesNew(LoaderBase loaderBase) throws IOException {
    loaderBase.initIterator();
    List<RowIndexRange> res = getTargetRowRange(loaderBase);

    Set<String> uniSeries = new HashSet<>();
    for (RowIndexRange rir : res) {
      uniSeries.add(loaderBase.getIDString((int) rir.start) + "." + rir.vecName);
    }

    System.out.println(String.format("Record %d series for series query.", uniSeries.size()));
    singleSeries.addAll(uniSeries);
  }

  private void sampleTimeRangesNew(LoaderBase loaderBase) throws IOException {
    loaderBase.initIterator();
    List<RowIndexRange> res = getTargetRowRange(loaderBase);

    for (RowIndexRange rir : res) {
      timeRanges.add(new TimeRange(
          loaderBase.getIDString((int) rir.start) + "." + rir.vecName,
          loaderBase.getTS((int) rir.start),
          loaderBase.getTS((int) rir.end)
      ));
    }

    System.out.println("Finish sampling time-ranges.");
  }

  public static void generateConditionFile(MergedDataSets ds) throws IOException, ClassNotFoundException {
    LoaderBase loaderBase = LoaderBase.getLoader(ds);
    ConditionGeneratorV2 cg = new ConditionGeneratorV2();
    cg.sampleAlignedNew(loaderBase);
    cg.sampleSeriesNew(loaderBase);
    cg.sampleTimeRangesNew(loaderBase);

    // this last one is special as could be little distinct ranges
    do {
      cg.doubleRanges.clear();
      cg.sampleDoubleRangesNew(loaderBase);
      CONDITION_CARD *= 2;
    } while (cg.doubleRanges.size() < 10);
    serialize(cg, ds.getNewConditionFile());
    loaderBase.close();
  }

  public static void main(String[] args) throws IOException, ClassNotFoundException {
    // MergedDataSets dataSets = MergedDataSets.CCS;
    // generateConditionFile(dataSets);

    for (MergedDataSets ds : MergedDataSets.values()) {
      CONDITION_CARD = 100;
      // generateConditionFile(ds);
      System.out.println(String.format("FINISH %s", ds.name()));
      ConditionGeneratorV2 cg2 = deserialize(ds.getNewConditionFile());
      System.out.println("Check conditions.");
    }
    // LoaderBase loaderBase = LoaderBase.getNewLoader(dataSets);
    // loaderBase.initIterator();
    // List<RowIndexRange> res = getTargetRowRange(loaderBase);
    // loaderBase.close();
    System.out.println("Verified.");

  }

  // region Core Internal

  private static class RowIndexRange {
    long start, end;
    String vecName;
    int valStr, valEnd;

    public RowIndexRange(long start, long end, String vn, int vs, int ve) {
      this.start = start;
      this.end = end;
      this.vecName = vn;
      this.valStr = vs;
      this.valEnd = ve;
    }

    @Override
    public String toString() {
      return String.format("%d -> %d", start, end);
    }
  }

  /**
   * The key to generate condition, is to fix the target rows.
   * With rows fixed, related timestamps or columns value are easy to fetch.
   * Guarantees that selected ranges are identical to each other.
   */
  private static List<RowIndexRange> getTargetRowRange(LoaderBase loader) throws IOException {
    final long ttlRow = loader.getTotalRows();
    final long span = ttlRow / CONDITION_CARD;
    int seed = new Random().nextInt((int) (span));

    // seed as the start point
    int startIdx = 0;
    List<RowIndexRange> res = new ArrayList<>();
    Set<Integer> existStartIdx = new HashSet<>();

    ProgressReporter reporter = new ProgressReporter(CONDITION_CARD, "collect_range");
    reporter.setInterval(0.8f);
    while (res.size() < CONDITION_CARD) {
      int disturb = new Random().nextInt((int) (seed * 0.05));  // to prevent dead loop
      startIdx = (int) (startIdx + span + disturb);
      startIdx = (int) (startIdx % ttlRow);

      // each for loop fins a range
      for (;;) {
        String curID = loader.getIDString(startIdx);
        int step = 0;
        while (step < RANGE_SPAN) {
          if (curID.equals(loader.getIDString(startIdx + step))) {
            step++;
          } else {
            startIdx += step;
            startIdx = (int) (startIdx % ttlRow);
            break;
          }
        }

        String tarVec = null;
        if (step == RANGE_SPAN && !existStartIdx.contains(startIdx)
            && (tarVec = getValidDFVector(loader, startIdx, startIdx + step)) != null) {
          res.add(new RowIndexRange(startIdx, startIdx + step, tarVec, VAL_STR, VAL_END));
          existStartIdx.add(startIdx);
          reporter.addProgressAndReport(1);
          break;
        }
      }
    }
    return res;
  }

  private static int VAL_STR = -1, VAL_END = -1;  // only as passing parameter for above and following method
  private final static int VALID_LEN_THRESHOLD = 10;

  /**
   * return the vector which is DOUBLE/FLOAT and has non-null values within the range.
   * DF for Double or Float.
   * Only guarantees selected vectors is not null within the range.
   */
  private static String getValidDFVector(LoaderBase loader, int si, int ei) throws IOException {
    // check device consistency
    String dev = loader.getIDString(si);
    if (!dev.equals(loader.getIDString(ei))) {
      System.out.println("Device change during collected range.");
      System.exit(-1);
    }

    Set<String> vn = loader.getRelatedSensors(dev);
    List<FieldVector> allVec = loader.getVectors();
    List<FieldVector> canVec = new ArrayList<>();
    // collect all related vector which is float or double type
    for (FieldVector fv : allVec) {
      if ( vn.contains(fv.getField().getName())
          && fv.getField().getFieldType().getType().getTypeID() == ArrowType.ArrowTypeID.FloatingPoint) {
        canVec.add(fv);
      }
    }

    Collections.shuffle(canVec);
    // locate a vector has more than THRESHOLD non-null DF values
    for (FieldVector fv : canVec) {
      for (int idx = si ; idx <= ei; ) {
        VAL_END = VAL_STR = -1;
        if (fv.isNull(loader.reloadAndGetLocalIndex(idx))) {
          idx++;
          continue;
        }

        int valCnt = 1;
        VAL_STR = idx;
        while (valCnt <= VALID_LEN_THRESHOLD) {
          if (!fv.isNull(loader.reloadAndGetLocalIndex(idx + valCnt))) {
            valCnt ++;
          } else {
            idx += valCnt;
            break;
          }
        }

        if (valCnt > VALID_LEN_THRESHOLD) {
          VAL_END = valCnt + idx - 1;
          return fv.getName();
        }
      }
    }
    return null;
  }

  // endregion

}
