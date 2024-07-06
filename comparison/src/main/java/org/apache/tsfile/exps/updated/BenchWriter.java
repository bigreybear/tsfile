package org.apache.tsfile.exps.updated;

import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.exps.conf.FileScheme;
import org.apache.tsfile.exps.conf.MergedDataSets;
import org.apache.tsfile.exps.utils.ResultPrinter;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class BenchWriter {
  public static int BATCH = 1024;
  public static boolean TO_PROFILE = true;

  public static class DataSetsProfile {
    public static int deviceNum = 0;
    public static int rowNum = 0;
  }

  // configure alignment of the tablet
  static void writeTablet(TsFileWriter writer, Tablet tablet) throws IOException, WriteProcessException {
    if (_tsfileAlignedTablet) {
      writer.writeAligned(tablet);
    } else {
      writer.write(tablet);
    }
  }

  static void registerTablet(TsFileWriter writer, String curDev, List<MeasurementSchema> msl) throws WriteProcessException {
    if (_tsfileAlignedTablet) {
      writer.registerAlignedTimeseries(Path.wrapDevPath(curDev), msl);
    } else {
      writer.registerTimeseries(Path.wrapDevPath(curDev), msl);
    }
  }

  public static void writeTsFile() throws IOException, WriteProcessException {
    loaderBase.initIterator();
    String preDev = new String(loaderBase.getID(0), StandardCharsets.UTF_8);

    currentScheme = FileScheme.TsFile;
    File tsFile = new File(_file_name + _tsfile_sf);
    tsFile.delete();

    TsFileWriter writer = new TsFileWriter(tsFile);

    loaderBase.updateDeviceID(preDev);
    List<MeasurementSchema> schemaList = loaderBase.getSchemaList();

    Tablet tablet = new Tablet("", schemaList, BATCH);
    tablet.initBitMaps();
    long[] timestamps = tablet.timestamps;
    loaderBase.initArrays(tablet);

    String curDev;
    tablet.setDeviceId(preDev);
    // Note(zx)
    registerTablet(writer, preDev, schemaList);

    int rowInTablet = 0;
    long lastTS = -1L;

    DataSetsProfile.deviceNum = 1;
    for (; loaderBase.hasNext(); loaderBase.next()) {
      curDev = new String(loaderBase.getID(), StandardCharsets.UTF_8);
      if (!preDev.equals(curDev)) {
        tablet.rowSize = rowInTablet;
        writeTablet(writer, tablet);
        tablet.reset();
        loaderBase.updateDeviceID(curDev);
        tablet = loaderBase.refreshTablet(tablet);
        timestamps = tablet.timestamps;
        loaderBase.refreshArrays(tablet);

        tablet.setDeviceId(curDev);
        registerTablet(writer, curDev, loaderBase.getSchemaList());
        rowInTablet = 0;

        if (TO_PROFILE) {
          DataSetsProfile.deviceNum ++;
        }

        preDev = curDev;
        lastTS = -1L;
      }

      if (rowInTablet >= BATCH) {
        tablet.rowSize = BATCH;
        writeTablet(writer, tablet);
        tablet.reset();
        tablet.setDeviceId(curDev);
        loaderBase.refreshArrays(tablet);
        rowInTablet = 0;
      }

      timestamps[rowInTablet] = loaderBase.getTS();
      if (timestamps[rowInTablet] <= lastTS) {
        continue;
      }

      lastTS = timestamps[rowInTablet];
      loaderBase.fillTablet(tablet, rowInTablet);
      rowInTablet ++;

      if (TO_PROFILE) {
        DataSetsProfile.rowNum ++;
      }
    }
    tablet.rowSize = rowInTablet;
    writeTablet(writer, tablet);
    writer.close();
    logger.setStatus(currentScheme, mergedDataSets);
    logger.writeResult(writer.report());
    logger.log(writer.verboseReport());
    // writer.report(logger);
  }

  private static void writeParquetInternal() throws IOException {
    File parFile = currentScheme.equals(FileScheme.Parquet)
        ?  new File(_file_name + _parquet_sf)
        :  new File(_file_name + _parquetas_sf);
    parFile.delete();

    ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(parFile.toPath()))
        .withConf(new PlainParquetConfiguration())
        .withType(loaderBase.getParquetSchema())
        .withCompressionCodec(compressorParquet)
        // .enableDictionaryEncoding()  // should or not?
        .build();

    // ParquetGroupFiller.setLoader(loaderBase);
    SimpleGroupFactory f= new SimpleGroupFactory(loaderBase.getParquetSchema());

    loaderBase.initIterator();
    String preDev = new String(loaderBase.getID(0), StandardCharsets.UTF_8);
    // ParquetGroupFiller.updateDeviceID(preDev);
    loaderBase.updateDeviceID(preDev);
    String curDev;
    // String[] nodes = preDev.split("\\.");

    List<Group> batchGroups = new ArrayList<>(BATCH);
    Group g;
    final int totalRows = loaderBase.getTotalRows();
    long lastTS = -1L, curTS; // filter out-of-order data

    for (; loaderBase.hasNext(); loaderBase.next()) {
      curDev = new String(loaderBase.getID(), StandardCharsets.UTF_8);
      if (!preDev.equals(curDev)) {

        if (TO_PROFILE) {
          DataSetsProfile.deviceNum++;
        }

        for (Group ig : batchGroups) {
          writer.write(ig);
        }
        batchGroups.clear();
        preDev = curDev;
        // ParquetGroupFiller.updateDeviceID(curDev);
        loaderBase.updateDeviceID(curDev);
        lastTS = -1;
      }


      curTS = loaderBase.getTS();
      if (curTS <= lastTS) {
        // rowInTablet not modified, next write is fine
        continue;
      }

      // g = ParquetGroupFiller.fill(f, loaderBase, cnt);
      g = loaderBase.fillGroup(f);

      batchGroups.add(g);

      lastTS = curTS;

      if (TO_PROFILE) {
        DataSetsProfile.rowNum ++;
      }
    }

    for (int i = 0; i < batchGroups.size(); i++) {
      writer.write(batchGroups.get(i));
    }

    writer.close();
    logger.setStatus(currentScheme, mergedDataSets);
    logger.writeResult(writer.report());
    // writer.report(logger);
  }

  public static void writeParquet() throws IOException {
    currentScheme = FileScheme.Parquet;
    writeParquetInternal();
  }

  // AS for alternated-schema
  public static void writeParquetAS() throws IOException {
    currentScheme = FileScheme.ParquetAS;
        switch (mergedDataSets) {
      case ZY:
      case REDD:
      case TSBS:
        break;
      // These are unavailable with alternated schema
      case GeoLife:
      case TDrive:
      default:
        return;
    }
    writeParquetInternal();
  }

  public static void writeArrowIPC() throws IOException {
    currentScheme = FileScheme.ArrowIPC;
    System.out.println("NOT WORK NOW");
  }


  public static void closeAndSummary() {
    System.out.println(logger.builder.toString());
  }


  public static MergedDataSets mergedDataSets;
  // one run write all schemes as this records
  public static FileScheme currentScheme;

  public static CompressionCodecName compressorParquet;
  public static CompressionType compressorTsFile;
  public static TSEncoding encodingTsFile = TSEncoding.GORILLA;
  public static boolean _tsfileAlignedTablet = true;

  public static ResultPrinter logger;

  static String _date_time = java.time.format.DateTimeFormatter
      .ofPattern("HHmmss")
      .format(java.time.LocalDateTime.now());
  static String _log_name = "merged_write_results.log";
  static String _file_name = "";

  static String _parquet_sf = ".parquet";
  static String _parquetas_sf = ".parquetas";
  static String _arrowipc = ".arrow";
  static String _tsfile_sf = ".tsfile";

  static LoaderBase loaderBase;


  static String[] _args = {"ZY", "SNAPPY"};  // pseudo input args
  /** Encoding is defined by {@link #encodingTsFile} for TsFile, and automated with Parquet.
   *  Compressor is defined by above parameters.
   *  Each run process a dataset. */
  public static void main(String[] args) throws IOException, WriteProcessException {
    if (args.length != 2) {
      args = _args;
    }

    mergedDataSets = MergedDataSets.valueOf(args[0]);
    compressorParquet = CompressionCodecName.valueOf(args[1]);
    compressorTsFile = CompressionType.valueOf(args[1]);
    _log_name = MergedDataSets.TARGET_DIR + _log_name;
    // logger = new BufferedWriter(new FileWriter(_log_name, true));
    logger = new ResultPrinter(_log_name, true);

    _file_name = "t-" + MergedDataSets.TARGET_DIR + mergedDataSets.name() + "_" + _date_time; // test at dev
    // _file_name = MergedDataSets.TARGET_DIR + mergedDataSets.name();  // run at exp

    // load once, use always
    loaderBase = LoaderBase.getLoader(mergedDataSets);

    // writeArrowIPC();
    // printProgress("Finish ArrowIPC");

    writeTsFile();
    printProgress("Finish TsFIle");

    writeParquet();
    printProgress("Finish Parquet");

    writeParquetAS();  // alternated-schema Parquet available IFF deviceID consists of multiple fields
    printProgress("Finish ParquetAS");

    closeAndSummary();
    logger.close();
  }

  public static void printProgress(String content) {
    String time = DateTimeFormatter.ofPattern("HH:mm:ss").format(java.time.LocalDateTime.now());
    System.out.println(String.format(
        "%s, at time: %s",
        content, time));
  }
}
