package org.apache.tsfile.exps;

import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.expression.IExpression;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.expression.impl.BinaryExpression;
import org.apache.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.tsfile.read.filter.factory.TimeFilterApi;
import org.apache.tsfile.read.filter.factory.ValueFilterApi;
import org.apache.tsfile.read.query.dataset.QueryDataSet;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BMReader {

  public static String DST_DIR = "F:\\0006DataSets\\Results\\";
  public static String DATE_STR = java.time.format.DateTimeFormatter
      .ofPattern("yyyyMMddHHmmss")
      .format(java.time.LocalDateTime.now());

  public static String NAME_COMMENT = "_NO_SPEC_";

  public static File tsFile;
  public static File logFile;

  public static BufferedWriter logger;

  // build expression objects for TsFile
  public static class QueryBuilder {
    public static List<QueryExpression> singleSeries(org.apache.tsfile.exps.ConditionGenerator cg) {
      List<QueryExpression> expressions = new ArrayList<>();
      for (String series : cg.singleSeries) {
        expressions.add(QueryExpression.create(Collections.singletonList(new Path(series)), null));
      }
      return expressions;
    }

    public static List<QueryExpression> alignedDevices(org.apache.tsfile.exps.ConditionGenerator cg, List<String> sensors) {
      List<QueryExpression> expressions = new ArrayList<>();
      for (String dev : cg.alignedDevices) {
        List<Path> series = new ArrayList<>();
        for (String s : sensors) {
          series.add(new Path(dev, s, false));
        }
        expressions.add(QueryExpression.create(series, null));
      }
      return expressions;
    }

    public static List<QueryExpression> timeExpression(org.apache.tsfile.exps.ConditionGenerator cg) {
      List<QueryExpression> expressions = new ArrayList<>();
      for (org.apache.tsfile.exps.ConditionGenerator.TimeRange tr : cg.timeRanges) {
        IExpression tFilter =
            BinaryExpression.and(
                new GlobalTimeExpression(TimeFilterApi.gtEq(tr.t1)),
                new GlobalTimeExpression(TimeFilterApi.ltEq(tr.t2)));
        expressions.add(QueryExpression.create(Collections.singletonList(new Path(tr.series)), tFilter));
      }
      return expressions;
    }

    public static List<QueryExpression> valueExpression(org.apache.tsfile.exps.ConditionGenerator cg) {
      List<QueryExpression> expressions = new ArrayList<>();
      for (org.apache.tsfile.exps.ConditionGenerator.DoubleRange dr: cg.doubleRanges) {
        IExpression tFilter =
            BinaryExpression.and(
                new SingleSeriesExpression(new Path(dr.series), ValueFilterApi.gtEq(dr.v1)),
                new SingleSeriesExpression(new Path(dr.series), ValueFilterApi.ltEq(dr.v2)));
        expressions.add(QueryExpression.create(Collections.singletonList(new Path(dr.series)), tFilter));
      }
      return expressions;
    }

    public static List<QueryExpression> crossExpression(org.apache.tsfile.exps.ConditionGenerator cg) {
      List<QueryExpression> expressions = new ArrayList<>();
      for (org.apache.tsfile.exps.ConditionGenerator.CrossRange cr : cg.crossRanges) {
        IExpression vFilter =
            BinaryExpression.and(
                new SingleSeriesExpression(new Path(cr.series), ValueFilterApi.gtEq(cr.v1)),
                new SingleSeriesExpression(new Path(cr.series), ValueFilterApi.ltEq(cr.v2)));
        expressions.add(QueryExpression.create(Collections.singletonList(new Path(cr.series)), vFilter));
      }
      return expressions;
    }

    public static List<QueryExpression> mixedExpression(org.apache.tsfile.exps.ConditionGenerator cg) {
      List<QueryExpression> expressions = new ArrayList<>();
      for (org.apache.tsfile.exps.ConditionGenerator.MixedRange mr : cg.mixedRanges) {
        IExpression tFilter =
            BinaryExpression.and(
                new GlobalTimeExpression(TimeFilterApi.gtEq(mr.t1)),
                new GlobalTimeExpression(TimeFilterApi.ltEq(mr.t2)));
        IExpression vFilter =
            BinaryExpression.and(
                new SingleSeriesExpression(new Path(mr.series), ValueFilterApi.gtEq(mr.v1)),
                new SingleSeriesExpression(new Path(mr.series), ValueFilterApi.ltEq(mr.v2)));

        IExpression expression = BinaryExpression.and(tFilter, vFilter);
        expressions.add(QueryExpression.create(Collections.singletonList(new Path(mr.series)), expression));
      }
      return expressions;
    }
  }

  public static ExecuteResult execute(TsFileReader reader, List<QueryExpression> exps) throws IOException {
    int cnt = 0;
    long time = System.nanoTime();
    for (QueryExpression e : exps) {
      QueryDataSet dataSet = reader.query(e);
      int first20 = 20;
      while (dataSet.hasNext() && first20 > 0) {
        dataSet.next();
        if (RECORD_LIMIT) {
          first20--;
        }
        // first20 --;
        cnt++;
      }
    }
    time = System.nanoTime() - time;
    System.out.println("Result count:" + cnt);
    return new ExecuteResult(time/exps.size(), cnt);
  }


  public static void bmQuery(TsFileReader reader, org.apache.tsfile.exps.ConditionGenerator cg, int queryType, int dataSets) throws IOException {
    long latency;
    List<QueryExpression> queryExpressions;
    switch (QueryType.values()[queryType]) {
      case SingleRaw:
        queryExpressions = QueryBuilder.singleSeries(cg);
        break;
      case AlignedRaw:
        queryExpressions = QueryBuilder.alignedDevices(cg, DataSets.values()[dataSets].sensors);
        break;
      case TimeFilter:
        queryExpressions = QueryBuilder.timeExpression(cg);
        break;
      case valueFilter:
        queryExpressions = QueryBuilder.valueExpression(cg);
        break;
      case MixedFilter:
        queryExpressions = QueryBuilder.mixedExpression(cg);
        break;
      case CrossFilter:
        queryExpressions = QueryBuilder.crossExpression(cg);
        break;
      default:
        queryExpressions = null;
    }

    ExecuteResult res = execute(reader, queryExpressions);
    formattedLog(DataSets.values()[dataSets].toString(), queryType, res.latency/1000000, res.rowCnt);
  }

  private static class ExecuteResult {
    long latency;
    int rowCnt; // to check answer
    public ExecuteResult (long l, int r) {
      latency = l; rowCnt = r;
    }
  }

  public static void init() throws IOException {
    tsFile = new File(FILE_PATH);
    logFile = new File(LOG_PATH);
    logger = new BufferedWriter(new FileWriter(logFile, true));
  }

  public static void formattedLog(String ds, int queryType, long latency, int rowCnt) throws IOException {
    logger.write(String.format("%s\t%s\t%s\t%d\t%d\t%s",
        ds, QueryType.values()[queryType], DATE_STR, latency, rowCnt, tsFile.getName()));
    logger.newLine();
  }

  public static DataSets DATA_SET = DataSets.TSBS; // datasets using distinct schemas
  public static String LOG_PATH = DST_DIR + "TS_FILE_Arity_Query_Results.log";
  public static String FILE_PATH = DST_DIR + DATA_SET.getTargetFile();
  public static boolean RECORD_LIMIT = true;
  public static void main(String[] args) throws IOException, ClassNotFoundException {
    int queryType = 0;
    FILE_PATH = DST_DIR + "TS_FILE_REDD_22145522_GORILLA_SNAPPY_arity1024.tsfile";
    DATA_SET = DataSets.REDD;
    if (args.length >= 3) {
      FILE_PATH = DST_DIR + args[0];
      DATA_SET = DataSets.valueOf(args[1]);
      queryType = Integer.parseInt(args[2]);
    } else {
      // System.out.println("Not enough arguments.");
      // return;
    }

    init();
    TsFileReader reader = new TsFileReader(new TsFileSequenceReader(FILE_PATH));
    org.apache.tsfile.exps.ConditionGenerator conditions = ConditionGenerator.getConditionsByDataSets(DATA_SET);
    bmQuery(reader, conditions, queryType, DATA_SET.ordinal());
    reader.close();
    // reader = new TsFileReader(new TsFileSequenceReader(FILE_PATH));
    // bmQuery(reader, conditions, 1, 0);
    // reader.close();
    logger.close();
  }

  public enum QueryType {
    SingleRaw,
    AlignedRaw,
    TimeFilter,
    valueFilter,
    MixedFilter,
    CrossFilter
  }

}