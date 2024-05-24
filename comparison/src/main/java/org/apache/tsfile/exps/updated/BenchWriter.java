package org.apache.tsfile.exps.updated;

import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.exps.conf.MergedDataSets;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class BenchWriter {
  public static int BATCH = 1024;
  public static boolean TO_PROFILE = true;

  public static class DataSetsProfile {
    public static int deviceNum = 0;
    public static int rowNum = 0;
  }

  public static void reportParquetProfile(ParquetWriter<?> writer) throws IOException {
    writer.report(logger);
  }

  public static void writeTsFile() throws IOException {
    File tsFile = new File(_file_name + _tsfile_sf);
    TsFileWriter writer = new TsFileWriter(tsFile);

    LoaderBase loaderBase = LoaderBase.getLoader(mergedDataSets);
    List<MeasurementSchema> schemaList = TsFileTabletFiller.getSchema();
    Tablet tablet = new Tablet("", schemaList);
    tablet.initBitMaps();
    long[] timestamps = tablet.timestamps;
    TsFileTabletFiller.initFiller(tablet, loaderBase);

    String preDev = new String(loaderBase.idVector.get(0), StandardCharsets.UTF_8);
    String curDev;
    tablet.setDeviceId(preDev);
    // Note(zx)
    // writer.registerTimeseries(new Path(preDev), );

  }

  public static void writeParquet() throws IOException {
    File parFile = new File(_file_name + _parquet_sf);
    ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(parFile.toPath()))
        .withConf(new PlainParquetConfiguration())
        .withType(mergedDataSets.getSchema())
        .withCompressionCodec(compressorParquet)
        .build();

    LoaderBase loaderBase = LoaderBase.getLoader(mergedDataSets);
    ParquetGroupFiller.setLoader(loaderBase);
    SimpleGroupFactory f= new SimpleGroupFactory(mergedDataSets.getSchema());

    String preDev = new String(loaderBase.idVector.get(0), StandardCharsets.UTF_8);
    ParquetGroupFiller.updateDeviceID(preDev);
    String curDev;
    // String[] nodes = preDev.split("\\.");

    List<Group> batchGroups = new ArrayList<>(BATCH);
    Group g;
    int totalRows = loaderBase.idVector.getValueCount();
    long lastTS = -1L, curTS; // filter out-of-order data

    for (int cnt = 0; cnt < totalRows; cnt++) {
      curDev = new String(loaderBase.idVector.get(cnt), StandardCharsets.UTF_8);
      if (!preDev.equals(curDev)) {

        if (TO_PROFILE) {
          DataSetsProfile.deviceNum++;
        }

        for (Group ig : batchGroups) {
          writer.write(ig);
        }
        batchGroups.clear();
        preDev = curDev;
        ParquetGroupFiller.updateDeviceID(curDev);
        lastTS = -1;
      }


      curTS = loaderBase.timestampVector.get(cnt);
      if (curTS <= lastTS) {
        // rowInTablet not modified, next write is fine
        continue;
      }

      g = ParquetGroupFiller.fill(f, loaderBase, cnt);

      batchGroups.add(g);

      lastTS = loaderBase.timestampVector.get(cnt);

      if (TO_PROFILE) {
        DataSetsProfile.rowNum ++;
      }
    }

    for (int i = 0; i < batchGroups.size(); i++) {
      writer.write(batchGroups.get(i));
    }

    writer.close();
    reportParquetProfile(writer);
  }


  public static void closeAndSummary() {
  }


  public static MergedDataSets mergedDataSets;
  public static CompressionCodecName compressorParquet;
  public static CompressionType compressorTsFile;
  public static TSEncoding encodingTsFile = TSEncoding.GORILLA;

  public static BufferedWriter logger;

  static String _date_time = java.time.format.DateTimeFormatter
      .ofPattern("yyyyMMddHHmmss")
      .format(java.time.LocalDateTime.now());
  static String _log_name = "merged_write_results.log";
  static String _file_name = "";

  static String _parquet_sf = ".parquet";
  static String _tsfile_sf = ".tsfile";

  static String[] _args = {"TDrive", "SNAPPY"};  // pseudo input args
  public static void main(String[] args) throws IOException {
    if (args.length != 2) {
      args = _args;
    }

    mergedDataSets = MergedDataSets.valueOf(args[0]);
    compressorParquet = CompressionCodecName.valueOf(args[1]);
    compressorTsFile = CompressionType.valueOf(args[1]);
    _log_name = MergedDataSets.TARGET_DIR + _log_name;
    logger = new BufferedWriter(new FileWriter(_log_name, true));
    _file_name = MergedDataSets.TARGET_DIR + mergedDataSets.name() + "_" + _date_time;

    // writeTsFile();
    writeParquet();
    closeAndSummary();
    logger.close();
  }
}
