package org.apache.tsfile.exps.updated;

import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.commons.collections.map.HashedMap;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.exps.conf.FileScheme;
import org.apache.tsfile.exps.conf.MergedDataSets;
import org.apache.tsfile.exps.utils.DevSenSupport;
import org.apache.tsfile.exps.utils.ResultPrinter;
import org.apache.tsfile.exps.utils.Stopwatch;
import org.apache.tsfile.exps.vector.ArrowVectorHelper;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.tsfile.exps.utils.TsFileSequentialConvertor.DICT_ID;

public class BenchWriter {
  public static int BATCH = 1024;
  public static int ARROW_BATCH = 65535;
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
    Stopwatch tabletConsTime = new Stopwatch();

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
    do {
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
      tabletConsTime.start();
      loaderBase.fillTablet(tablet, rowInTablet);
      tabletConsTime.stop();
      rowInTablet ++;

      if (TO_PROFILE) {
        DataSetsProfile.rowNum ++;
      }
    } while (loaderBase.next());
    tablet.rowSize = rowInTablet;
    writeTablet(writer, tablet);
    writer.close();
    logger.setStatus(currentScheme, mergedDataSets);
    logger.writeResult(writer.report());
    logger.log(writer.verboseReport());
    logger.log(String.format("Tablet construction time: %d", tabletConsTime.reportMilSecs()));
    System.out.println(String.format("Tablet construction time: %d", tabletConsTime.reportMilSecs()));
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

    do {
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
    } while (loaderBase.next());

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
      case CCS:
      case ZY:
      case REDD:
      case TSBS:
        break;
      // follows are unavailable with alternated schema
      case GeoLife:
      case TDrive:
      default:
        return;
    }
    writeParquetInternal();
  }

  /**
   * build schema vector via loader vector (nullable vectors)
   * build dictionary
   * open writer, with compressor setting
   * fill vector accords to dev-sen mapping: loop getID, choose wrk vec, fill in
   * once reach batch bound, write and record time
   * finish and report time
   */
  public static void writeArrowIPC() throws IOException {
    Stopwatch flushDataTime = new Stopwatch(), flushMetarTime = new Stopwatch();
    long dataSize = 0, metaSize = 0;

    // init
    loaderBase.initIterator();
    currentScheme = FileScheme.ArrowIPC;
    File arrFile = new File(_file_name + _arrowipc);
    BufferAllocator allocator = new RootAllocator(8 * 1024 * 1024 * 1024L);

    // Note(zx) build dictionary
    DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
    LargeVarCharVector dictVector = new LargeVarCharVector(Field.nullable("dict", Types.MinorType.LARGEVARCHAR.getType()), allocator);
    int cnt = 0;
    for (String s : loaderBase.getAllDevices()) {
      dictVector.setSafe(cnt, s.getBytes(StandardCharsets.UTF_8));
      cnt++;
    }
    dictVector.setValueCount(cnt);
    Dictionary dstIdDict = new Dictionary(dictVector, new DictionaryEncoding(DICT_ID, false, null));
    provider.put(dstIdDict);

    // build the root
    LargeVarCharVector tempIdVector = new LargeVarCharVector(Field.nullable("id", Types.MinorType.LARGEVARCHAR.getType()), allocator);
    Map<String, FieldVector> candidateVectors = new HashedMap(); // all vectors from the root
    VectorSchemaRoot refRoot = loaderBase.root;
    for (FieldVector rfv : refRoot.getFieldVectors()) {
      if (rfv.getName().equals("id")) {
        // different for coded id vector
        candidateVectors.put(
            "id",
            (FieldVector) DictionaryEncoder.encode(tempIdVector, dstIdDict)
        );
      } else {
        candidateVectors.put(
            rfv.getName(),
            ArrowVectorHelper.buildNullableVector(rfv.getField(), allocator));
      }
    }
    VectorSchemaRoot standingRoot = new VectorSchemaRoot(candidateVectors.values());
    try (FileOutputStream fos = new FileOutputStream(arrFile);
         FileChannel channel = fos.getChannel();
         ArrowFileWriter writer = compressorArrow == CompressionUtil.CodecType.NO_COMPRESSION
                               ?  new ArrowFileWriter(standingRoot, provider, channel)
                               :  new ArrowFileWriter(standingRoot,
                                                      provider,
                                                      channel,
                                                      null,
                                                      new IpcOption(),
                                                      new CommonsCompressionFactory(),
                                                      compressorArrow)) {
      writer.start();

      int sttFil = 0; // start of each filling batch
      int batIdx = 0; // index upon current batch

      // init working vectors
      // break when: 1. device changed; 2. internal vector runs out; 3. filling rows reach bounds
      for (;;) {
        int sob = loaderBase.getCurrentBatchCursor();  // Start Of the source Block
        sttFil = batIdx;
        BigIntVector tsVector = (BigIntVector) standingRoot.getVector("timestamp");
        while (batIdx < ARROW_BATCH) {
          tempIdVector.setSafe(batIdx, loaderBase.getID());
          tsVector.setSafe(batIdx, loaderBase.getTS());
          batIdx++;

          if (loaderBase.lastOneInBatch()) {
            break;
          }
          loaderBase.next();
        }

        tempIdVector.setValueCount(batIdx);
        tsVector.setValueCount(batIdx);
        // either last one from src block or dst bath full, fill whatever
        fillVectors(
            refRoot,
            standingRoot,
            sob,
            sttFil,
            batIdx - sttFil,
            tempIdVector,
            dstIdDict,
            loaderBase
        );
        standingRoot.setRowCount(batIdx);

        if (batIdx == ARROW_BATCH) {
          // write and continue
          flushDataTime.start();
          writer.writeBatch();
          flushDataTime.stop();

          batIdx = 0;

          standingRoot.getFieldVectors().forEach(ValueVector::reset);
          tempIdVector.reset();
        }

        if (loaderBase.lastOneInBatch()) {
          if (loaderBase.hasNext()) {
            // current block has been filled into the batch as above, fell free to load next block
            loaderBase.next();
          } else {
            // to break the whole loop
            break;
          }
        }
      }

      flushDataTime.start();
      writer.writeBatch();
      flushDataTime.stop();
      dataSize = writer.bytesWritten();

      flushMetarTime.start();
      writer.end();
      flushMetarTime.stop();
      metaSize = writer.bytesWritten() - dataSize;
    }
    candidateVectors.values().forEach(ValueVector::close);
    dictVector.close();
    tempIdVector.close();
    allocator.close();

    logger.writeResult(new long[] {dataSize, metaSize, flushDataTime.reportMilSecs(), flushMetarTime.reportMilSecs()});
  }

  // both begins are inclusive, while ends are exclusive
  private static void fillVectors(VectorSchemaRoot srcRoot,
                                  VectorSchemaRoot dstRoot,
                                  int srcBeg,
                                  int dstBeg,
                                  int steps,
                                  LargeVarCharVector dstCharIds,
                                  Dictionary dstDict,
                                  LoaderBase loaderBase) {

    IntVector codedIdVec = (IntVector) DictionaryEncoder.encode(dstCharIds, dstDict);

    for (int ofs = 0; ofs < steps; ) {
      int codedDev = codedIdVec.get(dstBeg + ofs);
      int span = 0;
      IntVector dstCodedIdVec = (IntVector) dstRoot.getVector("id");

      while (ofs + span < steps && codedDev == codedIdVec.get(span + ofs + dstBeg)) {
        dstCodedIdVec.setSafe(span + ofs + dstBeg, codedDev);
        span ++;
      }

      for (String vecName : loaderBase.getRelatedSensors(new String(dstCharIds.get(ofs + dstBeg), StandardCharsets.UTF_8))) {
        ArrowVectorHelper.setVectorInBatch(
            srcRoot.getVector(vecName),
            dstRoot.getVector(vecName),
            srcBeg + ofs,
            dstBeg + ofs,
            span);
      }

      ofs += span;
    }
    codedIdVec.close();
  }


  public static void closeAndSummary() {
    System.out.println(logger.builder.toString());
  }


  public static MergedDataSets mergedDataSets;
  // one run write all schemes as this records
  public static FileScheme currentScheme;

  public static CompressionCodecName compressorParquet;
  public static CompressionType compressorTsFile;
  public static CompressionUtil.CodecType compressorArrow;
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

  // preprocess: generate tsfile from legacy arrow
  public static void mainWrapper(String[] args) throws Exception{
    String[] parg = new String[0];
    // produce tsfile for convertor, which produce new_arrow_src
    // _args[0] = "GeoLife";
    // mainInternal(parg);
    //
    // _args[0] = "TDrive";
    // mainInternal(parg);
    //
    // _args[0] = "TSBS";
    // mainInternal(parg);
    //
    // _args[0] = "REDD";
    // mainInternal(parg);
    //
    // _args[0] = "ZY";
    // mainInternal(parg);
    //
    // _args[0] = "CCS";
    // mainInternal(parg);
  }

  static String[] _args = {"TDrive", "UNCOMPRESSED", "0111"};  // pseudo input args
  /** Encoding is defined by {@link #encodingTsFile} for TsFile, and automated with Parquet.
   *  Compressor is defined by above parameters.
   *  Each run process a dataset. */
  public static void main(String[] args) throws IOException, WriteProcessException, ClassNotFoundException {
    LoaderBase.USE_LEGACY_LOADER = false;

    if (args.length != 3) {
      args = _args;
    }

    boolean[] fileOption = new boolean[4];
    if (args[2].length() != 4) {
      System.out.println("Wrong with third flag option.");
      return;
    } else {
      for (int i = 0; i < args[2].length(); i++) {
        if (args[2].charAt(i) == '0') {
          fileOption[i] = false;
        } else if (args[2].charAt(i) == '1') {
          fileOption[i] = true;
        } else {
          System.out.println("Wrong code for file option.");
          return;
        }
      }
    }

    mergedDataSets = MergedDataSets.valueOf(args[0]);
    compressorParquet = CompressionCodecName.valueOf(args[1]);
    compressorTsFile = CompressionType.valueOf(args[1]);
    compressorArrow = getCompressorArrow(args[1]);
    String __log_name = MergedDataSets.TARGET_DIR + _log_name;
    // logger = new BufferedWriter(new FileWriter(_log_name, true));
    logger = new ResultPrinter(__log_name, true);

    _file_name = MergedDataSets.TARGET_DIR + args[0] + "_" + args[1]; // no time
    // _file_name = MergedDataSets.TARGET_DIR + args[0] + "_" + args[1] + "_" + _date_time; // test at dev

    // load once, use always
    loaderBase = LoaderBase.getLoader(mergedDataSets);

    if (fileOption[0]) {
      currentScheme = FileScheme.ArrowIPC;
      writeArrowIPC();
      printProgress("Finish ArrowIPC");
    }

    if (fileOption[1]) {
      currentScheme = FileScheme.TsFile;
      writeTsFile();
      printProgress("Finish TsFIle");
    }

    if (fileOption[2]) {
      currentScheme = FileScheme.Parquet;
      writeParquet();
      printProgress("Finish Parquet");
    }

    if (fileOption[3]) {
      currentScheme = FileScheme.ParquetAS;
      writeParquetAS();  // alternated-schema Parquet available IFF deviceID consists of multiple fields
      printProgress("Finish ParquetAS");
    }

    closeAndSummary();
    logger.close();
  }

  public static void printProgress(String content) {
    String time = DateTimeFormatter.ofPattern("HH:mm:ss").format(java.time.LocalDateTime.now());
    System.out.println(String.format(
        "%s, at time: %s",
        content, time));
  }

  public static CompressionUtil.CodecType getCompressorArrow(String comp) {
    switch (comp) {
      case "UNCOMPRESSED":
        return CompressionUtil.CodecType.NO_COMPRESSION;
      case "LZ4":
        return CompressionUtil.CodecType.LZ4_FRAME;
      case "ZSTD":
        return CompressionUtil.CodecType.ZSTD;
      default:
        throw new RuntimeException("No related compressor for Arrow.");
    }
  }
}
