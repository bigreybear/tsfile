package org.apache.tsfile.exps.updated;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.tsfile.encoding.encoder.TSEncodingBuilder;
import org.apache.tsfile.exps.BMReader;
import org.apache.tsfile.exps.ConditionGeneratorV2;
import org.apache.tsfile.exps.DataSets;
import org.apache.tsfile.exps.ParquetBMReader;
import org.apache.tsfile.exps.conf.FileScheme;
import org.apache.tsfile.exps.conf.MergedDataSets;
import org.apache.tsfile.exps.utils.DevSenSupport;
import org.apache.tsfile.exps.utils.ParquetQueryHelper;
import org.apache.tsfile.exps.utils.ResultPrinter;
import org.apache.tsfile.exps.utils.Stopwatch;
import org.apache.tsfile.exps.utils.TsFileQueryHelper;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.io.File;
import java.io.IOException;
import java.time.chrono.JapaneseEra;
import java.util.ArrayList;
import java.util.List;

public class BenchReader {

  public static void queryTsFile(ConditionGeneratorV2 con) throws IOException {

    String filePath = MergedDataSets.TARGET_DIR + QUERY_DATA_SET.name() + FILE_MIDDLE + ".tsfile";

    try (TsFileReader reader = new TsFileReader(new TsFileSequenceReader(filePath))) {
      List<QueryExpression> queryExpressions;
      switch (QTYPE) {
        case SingleRaw:
          queryExpressions = TsFileQueryHelper.QueryBuilder.singleSeries(con);
          break;
        case AlignedRaw:
          queryExpressions = TsFileQueryHelper.QueryBuilder.alignedDevices(con, DEV_SEN_MAP);
          break;
        case TimeFilter:
          queryExpressions = TsFileQueryHelper.QueryBuilder.timeExpression(con);
          break;
        case ValueFilter:
          TsFileQueryHelper.sensorTypes = reader.fileReader.getAllMeasurements();
          queryExpressions = TsFileQueryHelper.QueryBuilder.valueExpression(con);
          break;
        default:
          throw new RuntimeException("Wrong query type.");
      }
      Stopwatch latency = new Stopwatch();
      QueryDataSet dataSet;
      RowRecord rr;
      int recNum = 0;
      for (QueryExpression exp : queryExpressions) {
        latency.start();
        dataSet = reader.query(exp);
        while (dataSet.hasNext()) {
          dataSet.next();
          recNum ++;
        }
        latency.stop();
      }

      logger.queryResult(new long[] {latency.reportMilSecs(), queryExpressions.size(), recNum}, filePath);
    }

  }

  public static void queryParquet(ConditionGeneratorV2 con) throws IOException {
    queryParquetInternal(con, false);
  }

  public static void queryParquetAS(ConditionGeneratorV2 con) throws IOException {

    if (QUERY_DATA_SET == MergedDataSets.GeoLife || QUERY_DATA_SET == MergedDataSets.TDrive) {
      System.out.println("DeviceID with only one field has not to query parquet-as.");
      return;
    }

    queryParquetInternal(con, true);
  }

  private static void queryParquetInternal(ConditionGeneratorV2 condition,
                                           boolean alternateSchema) throws IOException {
    String fileName = alternateSchema
        ? QUERY_DATA_SET.name() + FILE_MIDDLE + ".parquetas"
        : QUERY_DATA_SET.name() + FILE_MIDDLE + ".parquet";
    fileName = MergedDataSets.TARGET_DIR + fileName;

    ParquetQueryHelper helper = new ParquetQueryHelper();
    helper.loadSchemaFromParquetFile(fileName);

    List<PlainParquetConfiguration> confList = new ArrayList<>();
    appendConfigurations(confList, condition, helper, alternateSchema);
    List<FilterCompat.Filter> queryExpressions = getParquetExpression(condition);

    ParquetReader.Builder builder = ParquetReader.builder(new GroupReadSupport(), new Path(fileName));
    if (confList.size() != queryExpressions.size()) {
      throw new RuntimeException("Discrepancy between schema and filter when query Parquet.");
    }

    // snippet to read actually
    final int ttl = confList.size();
    ParquetReader<Group> reader;
    Group record;
    Stopwatch latency = new Stopwatch();
    int recordsCount = 0;
    for (int i = 0; i < ttl; i++) {
      builder.withConf(confList.get(i));
      builder.withFilter(queryExpressions.get(i));

      reader = builder.build();
      latency.start();
      while ((record = reader.read()) != null) {
        recordsCount++;
      }
      latency.stop();
    }
    logger.queryResult(new long[] {latency.reportMilSecs(), ttl, recordsCount}, fileName);
  }

  // debug parameter
  // static List<Integer> rl1 = new ArrayList<>(), rl2 = new ArrayList<>();

  private static List<FilterCompat.Filter> getParquetExpression(ConditionGeneratorV2 condition) {
    // expression depends on parameters
    switch (QTYPE) {
      case SingleRaw:
        // notice this query may return group with null in target field, as
        //  that condition generator only guarantees part of it is not null.
        return ParquetQueryHelper.QueryBuilder.singleSeries(condition);
      case AlignedRaw:
        return ParquetQueryHelper.QueryBuilder.alignedDevices(condition);
      case TimeFilter:
        return ParquetQueryHelper.QueryBuilder.timeExpression(condition);
      case ValueFilter:
        return ParquetQueryHelper.QueryBuilder.valueExpression(condition);
      default:
        throw new UnSupportedDataTypeException("Wrong query type.");
    }
  }

  private static void appendConfigurations(List<PlainParquetConfiguration> lst,
                                           ConditionGeneratorV2 cond,
                                           ParquetQueryHelper helper,
                                           boolean as) {
    ParquetQueryHelper.QueryBuilder.ALTER_SCHEMA = as;
    switch (QTYPE) {
      case SingleRaw: {
        for (String path : cond.singleSeries) {
          String[] nodes = path.split("\\.");
          String sensor = nodes[nodes.length - 1];
          PlainParquetConfiguration conf = new PlainParquetConfiguration();
          conf.set(
              ReadSupport.PARQUET_READ_SCHEMA,
              helper.buildSingleFieldSchema(QUERY_DATA_SET, sensor, as).toString()
          );
          lst.add(conf);
        }
        return;
      }
      case AlignedRaw: {
        for (String dev : cond.alignedDevices) {
          PlainParquetConfiguration conf = new PlainParquetConfiguration();
          conf.set(
              ReadSupport.PARQUET_READ_SCHEMA,
              helper.buildAlignedSchema(QUERY_DATA_SET, as, DEV_SEN_MAP, dev).toString()
          );
          lst.add(conf);
        }
        return;
      }
      case TimeFilter:{
        for (ConditionGeneratorV2.TimeRange timeRange : cond.timeRanges) {
          String[] nodes = timeRange.series.split("\\.");
          String sensor = nodes[nodes.length - 1];
          PlainParquetConfiguration conf = new PlainParquetConfiguration();
          conf.set(
              ReadSupport.PARQUET_READ_SCHEMA,
              helper.buildSingleFieldSchema(QUERY_DATA_SET, sensor, as).toString()
          );
          lst.add(conf);
        }
        return;
      }
      case ValueFilter:{
        for (ConditionGeneratorV2.DoubleRange timeRange : cond.doubleRanges) {
          String[] nodes = timeRange.series.split("\\.");
          String sensor = nodes[nodes.length - 1];
          PlainParquetConfiguration conf = new PlainParquetConfiguration();
          conf.set(
              ReadSupport.PARQUET_READ_SCHEMA,
              helper.buildSingleFieldSchema(QUERY_DATA_SET, sensor, as).toString()
          );
          lst.add(conf);
        }
        return;
      }
      default:
        throw new UnSupportedDataTypeException("Wrong query type.");
    }
  }

  // test: run all over cases
  public static void main(String[] args) throws IOException, ClassNotFoundException {
    String[] parg = new String[0];
    for (MergedDataSets dataSets : MergedDataSets.values()) {
      _args[0] = dataSets.name();
      // for (int i = 0;i < 4; i++) {
        _args[1] = QueryType.values()[3].name();
        mainInternal(parg);
      // }
    }
  }


  // middle part of file to query
  public static String FILE_MIDDLE = "_UNCOMPRESSED";
  // query log file
  static String _log_name = "query_results.log";
  public static ResultPrinter logger;

  public static DevSenSupport DEV_SEN_MAP;
  public static MergedDataSets QUERY_DATA_SET;
  public static QueryType QTYPE;
  public static String[] _args = new String[] {"TDrive", QueryType.values()[3].name()};
  public static void mainInternal(String[] args) throws IOException, ClassNotFoundException {

    // init parameter
    if (args.length == 0) {
      args = _args;
    }

    if (args.length == 2) {
      QUERY_DATA_SET = MergedDataSets.valueOf(args[0]);
      QTYPE = QueryType.valueOf(args[1]);
    }

    String __log_name = MergedDataSets.TARGET_DIR + _log_name;
    // logger = new BufferedWriter(new FileWriter(_log_name, true));
    logger = new ResultPrinter(__log_name, true);

    DEV_SEN_MAP = DevSenSupport.deserialize(QUERY_DATA_SET.getNewSupport());
    ConditionGeneratorV2 condition = ConditionGeneratorV2.deserialize(QUERY_DATA_SET.getNewConditionFile());

    // query tsfile
    logger.setStatus(FileScheme.TsFile, QUERY_DATA_SET);
    queryTsFile(condition);

    // query parquet
    logger.setStatus(FileScheme.Parquet, QUERY_DATA_SET);
    queryParquet(condition);

    // query parquet-as
    logger.setStatus(FileScheme.ParquetAS, QUERY_DATA_SET);
    queryParquetAS(condition);

    // debug
    // for (int i = 0; i < rl1.size(); i++) {
    //   System.out.println(String.format("%d %d %d",
    //       i, rl1.get(i), rl2.get(i)));
    // }

    logger.close();
  }

  public enum QueryType {
    SingleRaw,
    AlignedRaw,
    TimeFilter,
    ValueFilter
  }
}
