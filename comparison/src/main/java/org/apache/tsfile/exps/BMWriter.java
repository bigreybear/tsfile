package org.apache.tsfile.exps;

import org.apache.arrow.vector.Float8Vector;
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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BMWriter {

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
      .ofPattern("yyyyMMddHHmmss")
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
    tsFile = new File(FILE_PATH);
    logFile = new File(LOG_PATH);

    writer = new TsFileWriter(tsFile);
    logger = new BufferedWriter(new FileWriter(logFile, true));
  }

  public static void closeAndSummary() throws IOException {
    writer.close();
    logger.write(String.format("Load: %s, Comment:%s, File:%s", CUR_DATA, NAME_COMMENT, tsFile.getName()));
    logger.newLine();
    logger.write(String.format("Pts: %d, Series: %d, Devs: %d\n",
        DataSetsProfile.ptsNum, DataSetsProfile.seriesNum, DataSetsProfile.deviceNum));
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
    org.apache.tsfile.exps.REDDLoader loader = REDDLoader.deserialize(CUR_DATA.getArrowFile());
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
        tablet.setDeviceId(curDev);
        writer.registerAlignedTimeseries(new Path(curDev), schemaList);
        rowInTablet = 0;

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
    return loader;
  }

  public static void checker() throws IOException {
    TsFileReader reader = new TsFileReader(new TsFileSequenceReader(FILE_PATH));
    ArrayList<Path> paths = new ArrayList<>();
    paths.add(new Path("root.010_100", "lat", false));
    paths.add(new Path("root.010_100", "lon", false));

    queryAndPrint(paths, reader, null);
  }

  private static void queryAndPrint(ArrayList<Path> paths, TsFileReader readTsFile, IExpression statement)
      throws IOException {
    QueryExpression queryExpression = QueryExpression.create(paths, statement);
    QueryDataSet queryDataSet = readTsFile.query(queryExpression);
    while (queryDataSet.hasNext()) {
      System.out.println(queryDataSet.next());
    }
    System.out.println("------------");
  }

  private static void commentOnName(String comment) {
    if (comment != null) {
      NAME_COMMENT = "_" + comment + "";
      FILE_PATH = DST_DIR + "TS_FILE_" + CUR_DATA + "_" + DATE_STR + NAME_COMMENT + ".tsfile";
      // LOG_PATH = DST_DIR + "TS_FILE_" + "_" + CUR_DATA + "_" + DATE_STR + NAME_COMMENT + ".log";
    }
  }


  public static DataSets CUR_DATA = DataSets.GeoLife;
  public static String FILE_PATH = DST_DIR + "TS_FILE_" + CUR_DATA + "_" + DATE_STR + ".tsfile";
  public static String LOG_PATH = DST_DIR + "TS_FILE_Write_Results.log";
  public static void main(String[] args) throws IOException, WriteProcessException {
    // assign default
    compressionType = CompressionType.SNAPPY;
    encoding = TSEncoding.GORILLA;

    // assign from args, args: [dataset]
    if (args.length >= 1) {
      CUR_DATA = DataSets.valueOf(args[0]);
    }

    commentOnName(encoding.name() + "_" + compressionType.name());
    init();

    long record = System.currentTimeMillis();
    // write as needed
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

    record = System.currentTimeMillis() - record;
    System.out.println(String.format("Load and write for %d ms.", record));
    closeAndSummary();
    // checker();
    logger.close();
  }
}
