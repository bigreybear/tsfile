package org.apache.tsfile.exps.utils;

import org.apache.tsfile.exps.ConditionGeneratorV2;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.expression.IExpression;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.expression.impl.BinaryExpression;
import org.apache.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.tsfile.read.filter.factory.TimeFilterApi;
import org.apache.tsfile.read.filter.factory.ValueFilterApi;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TsFileQueryHelper {
  public static class QueryBuilder {
    public static List<QueryExpression> singleSeries(ConditionGeneratorV2 cg) {
      List<QueryExpression> expressions = new ArrayList<>();
      for (String series : cg.singleSeries) {
        expressions.add(QueryExpression.create(Collections.singletonList(new Path(series)), null));
      }
      return expressions;
    }

    public static List<QueryExpression> alignedDevices(ConditionGeneratorV2 cg, DevSenSupport support) {
      List<QueryExpression> expressions = new ArrayList<>();
      for (String dev : cg.alignedDevices) {
        List<Path> series = new ArrayList<>();
        for (String s : support.map.get(dev)) {
          series.add(new Path(dev, s, false));
        }
        expressions.add(QueryExpression.create(series, null));
      }
      return expressions;
    }

    public static List<QueryExpression> timeExpression(ConditionGeneratorV2 cg) {
      List<QueryExpression> expressions = new ArrayList<>();
      for (ConditionGeneratorV2.TimeRange tr : cg.timeRanges) {
        IExpression tFilter =
            BinaryExpression.and(
                new GlobalTimeExpression(TimeFilterApi.gtEq(tr.t1)),
                new GlobalTimeExpression(TimeFilterApi.ltEq(tr.t2)));
        expressions.add(QueryExpression.create(Collections.singletonList(new Path(tr.series)), tFilter));
      }
      return expressions;
    }

    public static List<QueryExpression> valueExpression(ConditionGeneratorV2 cg) {
      List<QueryExpression> expressions = new ArrayList<>();
      for (ConditionGeneratorV2.DoubleRange dr: cg.doubleRanges) {
        IExpression tFilter =
            BinaryExpression.and(
                new SingleSeriesExpression(new Path(dr.series), ValueFilterApi.gtEq(dr.v1)),
                new SingleSeriesExpression(new Path(dr.series), ValueFilterApi.ltEq(dr.v2)));
        expressions.add(QueryExpression.create(Collections.singletonList(new Path(dr.series)), tFilter));
      }
      return expressions;
    }

    public static List<QueryExpression> crossExpression(ConditionGeneratorV2 cg) {
      List<QueryExpression> expressions = new ArrayList<>();
      for (ConditionGeneratorV2.CrossRange cr : cg.crossRanges) {
        IExpression vFilter =
            BinaryExpression.and(
                new SingleSeriesExpression(new Path(cr.series), ValueFilterApi.gtEq(cr.v1)),
                new SingleSeriesExpression(new Path(cr.series), ValueFilterApi.ltEq(cr.v2)));
        expressions.add(QueryExpression.create(Collections.singletonList(new Path(cr.series)), vFilter));
      }
      return expressions;
    }

    public static List<QueryExpression> mixedExpression(ConditionGeneratorV2 cg) {
      List<QueryExpression> expressions = new ArrayList<>();
      for (ConditionGeneratorV2.MixedRange mr : cg.mixedRanges) {
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
}
