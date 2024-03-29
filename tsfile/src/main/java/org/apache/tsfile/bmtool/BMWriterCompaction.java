package org.apache.tsfile.bmtool;

import org.apache.arrow.vector.Float8Vector;
// import org.apache.iotdb.commons.conf.IoTDBConstant;
// import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICompactionPerformer;
// import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.subtask.FastCompactionTaskSummary;
// import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
// import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
// import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
// import org.apache.iotdb.db.utils.constant.TestConstant;
// import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.expression.IExpression;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
// import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.tsfile.write.writer.TsFileIOWriter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// import static org.apache.iotdb.commons.conf.IoTDBConstant.FILE_NAME_SEPARATOR;
// import static org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator.getTsFileName;

public class BMWriterCompaction {

  public static class DataSetsProfile {
    public static int deviceNum = 0;
    public static int seriesNum = 0;
    public static long ptsNum = 0;

    public static long singleSeriesLatency;
    public static long alignedSeriesLatency;
    public static long timeQueryLatency;
    public static long valueQueryLatency;
    public static long mixedQueryLatency;
  }

  public static boolean TO_PROFILE = true;

  public static String DST_DIR = "F:\\0006DataSets\\Results\\";
  public static String DATE_STR = java.time.format.DateTimeFormatter
      .ofPattern("ddHHmmss")
      .format(java.time.LocalDateTime.now());

  public static String NAME_COMMENT = "_NO_SPEC_";

  public static File tsFile;
  public static File logFile;

  public static TsFileWriter writer;
  public static BufferedWriter logger;

  public static int BATCH = 1024;
  public static CompressionType compressionType;
  public static TSEncoding encoding;

  public static void init() throws IOException {
    // tsFile = new File(FILE_PATH);
    logFile = new File(LOG_PATH);

    // writer = new TsFileWriter(tsFile);
    logger = new BufferedWriter(new FileWriter(logFile, true));
  }

  public static void closeAndSummary() throws IOException {
    writer.close();
    logger.write(String.format("Load: %s, Comment:%s, File:%s", CUR_DATA, NAME_COMMENT, tsFile.getName()));
    logger.newLine();
    logger.write(String.format("Pts: %d, Series: %d, Devs: %d, Arity: %d\n",
        DataSetsProfile.ptsNum, DataSetsProfile.seriesNum, DataSetsProfile.deviceNum,
        TSFileConfig.maxDegreeOfIndexNode));
    writer.report(logger);
    logger.write("==========================================");
    logger.newLine();
  }

  public static void bmTSBS() throws IOException, WriteProcessException {
    TSBSLoader loader = TSBSLoader.deserialize(CUR_DATA.getArrowFile());
    List<MeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("lat", TSDataType.DOUBLE, encoding, compressionType));
    schemaList.add(new MeasurementSchema("lon", TSDataType.DOUBLE, encoding, compressionType));
    schemaList.add(new MeasurementSchema("ele", TSDataType.DOUBLE, encoding, compressionType));
    schemaList.add(new MeasurementSchema("vel", TSDataType.DOUBLE, encoding, compressionType));

    Tablet tablet = new Tablet("", schemaList, BATCH);
    tablet.initBitMaps();
    long[] timestamps = tablet.timestamps;
    double[] lats = (double[]) tablet.values[0];
    double[] lons = (double[]) tablet.values[1];
    double[] eles = (double[]) tablet.values[2];
    double[] vels = (double[]) tablet.values[3];

    String preDev = new String(loader.idVector.get(0), TSFileConfig.STRING_CHARSET);
    String curDev = null;
    tablet.setDeviceId(preDev);
    writer.registerAlignedTimeseries(new Path(preDev), schemaList);

    int totalRows = loader.idVector.getValueCount();
    int rowInTablet = 0;
    long lastTS = -1L; // filter out-of-order data
    for (int cnt = 0; cnt < totalRows; cnt++) {
      curDev = new String(loader.idVector.get(cnt), TSFileConfig.STRING_CHARSET);
      if (!preDev.equals(curDev)) {
        tablet.rowSize = rowInTablet;
        writer.writeAligned(tablet);
        tablet.reset();
        timestamps = tablet.timestamps;
        lats = (double[]) tablet.values[0];
        lons = (double[]) tablet.values[1];
        eles = (double[]) tablet.values[2];
        vels = (double[]) tablet.values[3];

        tablet.setDeviceId(curDev);
        writer.registerAlignedTimeseries(new Path(curDev), schemaList);
        rowInTablet = 0;

        if (TO_PROFILE) {
          DataSetsProfile.deviceNum++;
          DataSetsProfile.seriesNum += 4;
        }

        preDev = curDev;
        lastTS = -1;
      }

      if (rowInTablet >= BATCH) {
        tablet.rowSize = BATCH;
        writer.writeAligned(tablet);
        tablet.reset();
        tablet.setDeviceId(curDev);
        lats = (double[]) tablet.values[0];
        lons = (double[]) tablet.values[1];
        eles = (double[]) tablet.values[2];
        vels = (double[]) tablet.values[3];
        rowInTablet = 0;
      }

      timestamps[rowInTablet] = loader.timestampVector.get(cnt);
      if (timestamps[rowInTablet] <= lastTS) {
        // rowInTablet not modified, next write is fine
        continue;
      }
      lastTS = timestamps[rowInTablet];
      setValueWithNull(lats, tablet.bitMaps[0], cnt, rowInTablet, loader.latVec);
      setValueWithNull(lons, tablet.bitMaps[1], cnt, rowInTablet, loader.lonVec);
      setValueWithNull(eles, tablet.bitMaps[2], cnt, rowInTablet, loader.eleVec);
      setValueWithNull(vels, tablet.bitMaps[3], cnt, rowInTablet, loader.velVec);

      rowInTablet ++;

      if (TO_PROFILE) {
        DataSetsProfile.ptsNum += 4;
      }
    }
    tablet.rowSize = rowInTablet;
    writer.writeAligned(tablet);
  }

  public static void setValueWithNull(double[] dvs, BitMap bm, int vecIdx, int tabIdx, Float8Vector vec) {
    if (vec.isNull(vecIdx)) {
      bm.mark(tabIdx);
    } else {
      dvs[tabIdx] = vec.get(vecIdx);
    }
  }

  public static void bmREDD() throws IOException, WriteProcessException {
    REDDLoader loader = REDDLoader.deserialize(CUR_DATA.getArrowFile());
    List<MeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("elec", TSDataType.DOUBLE, encoding, compressionType));

    Tablet tablet = new Tablet("", schemaList, BATCH);
    long[] timestamps = tablet.timestamps;
    double[] elecs = (double[]) tablet.values[0];

    String preDev = new String(loader.idVector.get(0), TSFileConfig.STRING_CHARSET);
    String curDev = null;
    tablet.setDeviceId(preDev);
    writer.registerTimeseries(new Path(preDev), schemaList);

    int totalRows = loader.idVector.getValueCount();
    int rowInTablet = 0;
    long lastTS = -1L; // filter out-of-order data
    for (int cnt = 0; cnt < totalRows; cnt++) {
      curDev = new String(loader.idVector.get(cnt), TSFileConfig.STRING_CHARSET);
      if (!preDev.equals(curDev)) {
        tablet.rowSize = rowInTablet;
        writer.write(tablet);
        tablet.reset();
        timestamps = tablet.timestamps;
        elecs = (double[]) tablet.values[0];
        tablet.setDeviceId(curDev);
        writer.registerTimeseries(new Path(curDev), schemaList);
        rowInTablet = 0;

        if (TO_PROFILE) {
          DataSetsProfile.deviceNum++;
          DataSetsProfile.seriesNum += 1;
        }

        preDev = curDev;
        lastTS = -1;
      }

      if (rowInTablet >= BATCH) {
        tablet.rowSize = BATCH;
        writer.write(tablet);
        tablet.reset();
        tablet.setDeviceId(curDev);
        elecs = (double[]) tablet.values[0];
        rowInTablet = 0;
      }

      timestamps[rowInTablet] = loader.timestampVector.get(cnt);
      if (timestamps[rowInTablet] <= lastTS) {
        // rowInTablet not modified, next write is fine
        continue;
      }
      lastTS = timestamps[rowInTablet];
      elecs[rowInTablet] = loader.elecVector.get(cnt);
      rowInTablet ++;

      if (TO_PROFILE) {
        DataSetsProfile.ptsNum += 1;
      }
    }
    tablet.rowSize = rowInTablet;
    writer.write(tablet);
  }

  public static void bmGeoLife() throws IOException, WriteProcessException {
    GeoLifeLoader loader = GeoLifeLoader.deserialize(CUR_DATA.getArrowFile());
    List<MeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("lat", TSDataType.DOUBLE, encoding, compressionType));
    schemaList.add(new MeasurementSchema("lon", TSDataType.DOUBLE, encoding, compressionType));
    schemaList.add(new MeasurementSchema("alt", TSDataType.DOUBLE, encoding, compressionType));

    Tablet tablet = new Tablet("", schemaList, BATCH);
    long[] timestamps = tablet.timestamps;
    double[] lats = (double[]) tablet.values[0];
    double[] lons = (double[]) tablet.values[1];
    double[] alts = (double[]) tablet.values[2];

    String preDev = new String(loader.idVector.get(0), TSFileConfig.STRING_CHARSET);
    String curDev = null;
    tablet.setDeviceId(preDev);
    writer.registerTimeseries(new Path(preDev), schemaList);

    int totalRows = loader.idVector.getValueCount();
    int rowInTablet = 0;
    long lastTS = -1L; // filter out-of-order data
    for (int cnt = 0; cnt < totalRows; cnt++) {
      curDev = new String(loader.idVector.get(cnt), TSFileConfig.STRING_CHARSET);
      if (!preDev.equals(curDev)) {
        tablet.rowSize = rowInTablet;
        writer.write(tablet);
        tablet.reset();
        timestamps = tablet.timestamps;
        lats = (double[]) tablet.values[0];
        lons = (double[]) tablet.values[1];
        alts = (double[]) tablet.values[2];
        tablet.setDeviceId(curDev);
        writer.registerTimeseries(new Path(curDev), schemaList);
        rowInTablet = 0;

        if (TO_PROFILE) {
          DataSetsProfile.deviceNum++;
          DataSetsProfile.seriesNum += 3;
        }

        preDev = curDev;
        lastTS = -1;
      }

      if (rowInTablet >= BATCH) {
        tablet.rowSize = BATCH;
        writer.write(tablet);
        tablet.reset();
        tablet.setDeviceId(curDev);
        lats = (double[]) tablet.values[0];
        lons = (double[]) tablet.values[1];
        alts = (double[]) tablet.values[2];
        rowInTablet = 0;
      }

      timestamps[rowInTablet] = loader.timestampVector.get(cnt);
      if (timestamps[rowInTablet] <= lastTS) {
        // rowInTablet not modified, next write is fine
        continue;
      }
      lastTS = timestamps[rowInTablet];
      lats[rowInTablet] = loader.latitudeVector.get(cnt);
      lons[rowInTablet] = loader.longitudeVector.get(cnt);
      alts[rowInTablet] = loader.altitudeVector.get(cnt);
      rowInTablet ++;

      if (TO_PROFILE) {
        DataSetsProfile.ptsNum += 3;
      }
    }
    tablet.rowSize = rowInTablet;
    writer.write(tablet);
  }

  public static TDriveLoader bmTDrive() throws IOException, WriteProcessException {

    TDriveLoader loader = TDriveLoader.deserialize(CUR_DATA.getArrowFile());
    // loader.load(Long.MAX_VALUE);
    List<MeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("lat", TSDataType.DOUBLE, encoding, compressionType));
    schemaList.add(new MeasurementSchema("lon", TSDataType.DOUBLE, encoding, compressionType));
    long clock;

    Tablet tablet = new Tablet("", schemaList, BATCH);
    long[] timestamps = tablet.timestamps;
    double[] lats = (double[]) tablet.values[0];
    double[] lons = (double[]) tablet.values[1];

    String preDev = new String(loader.idVector.get(0), TSFileConfig.STRING_CHARSET);
    String curDev = null;
    tablet.setDeviceId(preDev);
    writer.registerAlignedTimeseries(new Path(preDev), schemaList);

    startDevTime.put(preDev, loader.timestampVector.get(0));

    int totalRows = loader.idVector.getValueCount();
    int rowInTablet = 0;
    long lastTS = -1L; // filter out-of-order data
    for (int cnt = 0; cnt < totalRows; cnt++) {
      curDev = new String(loader.idVector.get(cnt), TSFileConfig.STRING_CHARSET);
      if (cnt == totalRows/2) {
        writer.close();

        lastDevTime.put(preDev, loader.timestampVector.get(cnt));
        // resources.get(0).updateEndTime(lastDevTime);
        // updateAllStartTime(resources.get(0), startDevTime);
        lastDevTime.clear();
        startDevTime.clear();
        // resources.get(0).close();
        // resources.get(0).serialize();

        writer = getWriterFromResource(1);
        writer.registerAlignedTimeseries(new Path(curDev), schemaList);
      }

      if (!preDev.equals(curDev)) {
        lastDevTime.put(preDev, loader.timestampVector.get(cnt-1));

        tablet.rowSize = rowInTablet;
        writer.writeAligned(tablet);
        tablet.reset();
        timestamps = tablet.timestamps;
        lats = (double[]) tablet.values[0];
        lons = (double[]) tablet.values[1];
        tablet.setDeviceId(curDev);
        writer.registerAlignedTimeseries(new Path(curDev), schemaList);
        rowInTablet = 0;

        startDevTime.put(curDev, loader.timestampVector.get(cnt));

        if (TO_PROFILE) {
          DataSetsProfile.deviceNum++;
          DataSetsProfile.seriesNum += 2;
        }

        preDev = curDev;
        lastTS = -1;
      }

      if (rowInTablet >= BATCH) {
        tablet.rowSize = BATCH;
        writer.writeAligned(tablet);
        tablet.reset();
        tablet.setDeviceId(curDev);
        lats = (double[]) tablet.values[0];
        lons = (double[]) tablet.values[1];
        rowInTablet = 0;
      }

      timestamps[rowInTablet] = loader.timestampVector.get(cnt);
      if (timestamps[rowInTablet] <= lastTS) {
        // rowInTablet not modified, next write is fine
        continue;
      }
      lastTS = timestamps[rowInTablet];
      lats[rowInTablet] = loader.latitudeVector.get(cnt);
      lons[rowInTablet] = loader.longitudeVector.get(cnt);
      rowInTablet ++;

      if (TO_PROFILE) {
        DataSetsProfile.ptsNum += 2;
      }
    }
    tablet.rowSize = rowInTablet;
    writer.writeAligned(tablet);

    lastDevTime.put(preDev, loader.timestampVector.get(totalRows - 1));
    // resources.get(1).updateEndTime(lastDevTime);
    // updateAllStartTime(resources.get(1), startDevTime);

    return loader;
  }

  // public static void updateAllStartTime(TsFileResource resource, Map<String, Long> m) {
  //   for (Map.Entry<String, Long> entry : m.entrySet()) {
  //     resource.updateStartTime(entry.getKey(), 0);
  //   }
  // }

  // private static final ICompactionPerformer performer = new FastCompactionPerformer(false);
  // public static void compactUtilTDrive() throws Exception {
  //   TsFileResource targetResource =
  //       TsFileNameGenerator.getInnerCompactionTargetFileResource(resources, true);
  //   performer.setSourceFiles(resources);
  //   performer.setTargetFiles(Collections.singletonList(targetResource));
  //   performer.setSummary(new FastCompactionTaskSummary());
  //   performer.perform();
  // }

  public static void compactUtilTSBS() {

  }

  public static void compactUtilGeoLife() {

  }

  public static void compactUtilREDD() {

  }

  static Map<String, Long> lastDevTime = new HashMap<>();
  static Map<String, Long> startDevTime = new HashMap<>();

  private static void commentOnName(String comment) {
    if (comment != null) {
      NAME_COMMENT = "_" + comment + "";
      FILE_PATH = DST_DIR + "TS_FILE_" + CUR_DATA + "_" + DATE_STR + NAME_COMMENT + ".tsfile";
      // LOG_PATH = DST_DIR + "TS_FILE_" + "_" + CUR_DATA + "_" + DATE_STR + NAME_COMMENT + ".log";
    }
  }

  public static void buildResources() {
    dataDirectory = new File(
        SRC_DIR
            + "0".concat(File.separator)
            + "0".concat(File.separator));
    for (int i = 1; i < 3; i++) {
      // TsFileResource resource = new TsFileResource(
      //     new File(dataDirectory, String.format("%d-%d-0-0.tsfile", i, i)));
      // resources.add(resource);
    }
  }

  public static TsFileWriter getWriterFromResource(int i) throws IOException {
    // tsFile = resources.get(i).getTsFile();
    // return new TsFileWriter(new TsFileIOWriter(resources.get(i).getTsFile()));
    return null;
  }

  private static File dataDirectory;

  // public static List<TsFileResource> resources = new ArrayList<>();
  public static DataSets CUR_DATA = DataSets.TDrive;
  public static String SRC_DIR = "F:\\0006DataSets\\ForCompact\\";
  public static String FILE_PATH = DST_DIR + "TS_FILE_" + CUR_DATA + "_" + DATE_STR + ".tsfile";
  public static String LOG_PATH = DST_DIR + "TS_FILE_Compaction_Results.log";
  public static void main(String[] args) throws Exception {
    // assign default
    compressionType = CompressionType.SNAPPY;
    encoding = TSEncoding.GORILLA;
    boolean makeComponents = true;
    boolean compactWithUtil = true;
    boolean compactNative = false;

    // assign from args, args: [dataset, arity]
    if (args.length >= 2) {
      CUR_DATA = DataSets.valueOf(args[0]);
      TSFileConfig.maxDegreeOfIndexNode = Integer.parseInt(args[1]);
    } else {
      TSFileConfig.maxDegreeOfIndexNode = 1024;
      System.out.println("Not Enough Arguments");
    }
    SRC_DIR = SRC_DIR + CUR_DATA.toString() + "\\";

    buildResources();
    init();

    long record = System.currentTimeMillis();
    // build components
    if (makeComponents) {
      writer = getWriterFromResource(0);
      switch (CUR_DATA) {
        case TDrive:
          bmTDrive();
          // bmTDrive(200);
          break;
        case GeoLife:
          bmGeoLife();
          break;
        case REDD:
          bmREDD();
          break;
        case TSBS:
          bmTSBS();
          break;
        default:
      }
      writer.close();
      // resources.get(1).close();
      // resources.get(1).serialize();
    }

    if (compactWithUtil) {
      switch (CUR_DATA) {
        case TDrive:
          // compactUtilTDrive();
          // bmTDrive(200);
          break;
        case GeoLife:
          compactUtilGeoLife();
          break;
        case REDD:
          compactUtilREDD();
          break;
        case TSBS:
          compactUtilTSBS();
          break;
        default:
      }
    }

    // stopwatch for compacting

    record = System.currentTimeMillis() - record;
    System.out.println(String.format("Load and write for %d ms.", record));
    // closeAndSummary();
    // checker();
    logger.close();
  }
}
