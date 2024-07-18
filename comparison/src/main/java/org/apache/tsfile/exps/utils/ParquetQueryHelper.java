package org.apache.tsfile.exps.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.apache.tsfile.bmtool.ConditionGenerator;
import org.apache.tsfile.exps.ConditionGeneratorV2;
import org.apache.tsfile.exps.conf.MergedDataSets;
import org.apache.tsfile.exps.updated.BenchReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ParquetQueryHelper {

  MessageType messageType;
  Map<String, PrimitiveType> mapping = new HashMap<>();

  public ParquetQueryHelper() {}

  public MessageType loadSchemaFromParquetFile(String fileName) throws IOException {
    Configuration conf = new Configuration();
    Path path = new Path(fileName);

    try (ParquetFileReader reader = ParquetFileReader.open(conf, path)) {
      ParquetMetadata metadata = reader.getFooter();
      messageType = metadata.getFileMetaData().getSchema();

      // build field -> type mapping
      for (Type t : messageType.getFields()) {
        mapping.put(t.getName(), t.asPrimitiveType());
      }

      return messageType;
    }
  }

  public PrimitiveType getType(String name) {
    PrimitiveType type = mapping.get(name);
    return type;
  }

  // for aligned query
  public MessageType buildAlignedSchema(MergedDataSets dataSets, boolean as,
                                        DevSenSupport support, String dev) {
    List<Type> fields = new ArrayList<>();
    for (String sen : support.map.get(dev)) {
      Types.PrimitiveBuilder<?> tarBuilder =
          Types.optional(getType(sen).getPrimitiveTypeName());
      fields.add((Type) tarBuilder.named(sen));
    }
    return buildIDSchema(dataSets, fields, as);
  }

  // for single or filter query
  public MessageType buildSingleFieldSchema(MergedDataSets dataSets, String colName, boolean as) {
    List<Type> fields = new ArrayList<>();
    if (colName != null) {
      Types.PrimitiveBuilder<?> tarBuilder =
          Types.optional(getType(colName).getPrimitiveTypeName());
      fields.add((Type) tarBuilder.named(colName));
    }
    // build ID schema
    return buildIDSchema(dataSets, fields, as);
  }

  private MessageType buildIDSchema(MergedDataSets dataSets, List<Type> fields, boolean as) {
    if (as) {
      // only device ID and timestamp
      fields.add(
          Types.required(PrimitiveType.PrimitiveTypeName.BINARY).named("deviceID")
      );
      return new MessageType(dataSets.name(), fields);
    }

    // Note(zx) duplicate to loaders(V2), need better deduplication/cohesion
    Types.PrimitiveBuilder<PrimitiveType> idBuilder =
        Types.required(PrimitiveType.PrimitiveTypeName.BINARY);
    switch (dataSets) {
      case GeoLife:
      case TDrive: {
        fields.add(idBuilder.named("deviceID"));
        break;
      }
      case REDD: {
        fields.add(idBuilder.named("building"));
        fields.add(idBuilder.named("meter"));
        break;
      }
      case TSBS:{
        fields.add(idBuilder.named("fleet"));
        fields.add(idBuilder.named("name"));
        fields.add(idBuilder.named("driver"));
        break;
      }
      case ZY:
      case CCS: {
        fields.add(idBuilder.named("ent"));
        fields.add(idBuilder.named("dev"));
        break;
      }
    }
    return new MessageType(dataSets.name(), fields);
  }

  public static class QueryBuilder {

    public static boolean ALTER_SCHEMA = true;

    public static String getDeviceFromNodes(String[] array) {
      if (array == null || array.length == 0 ) {
        return "";
      }
      if (array.length == 2) {
        return array[0];
      }
      StringBuilder sb = new StringBuilder(array[0]);
      for (int i = 1; i < array.length - 1; i++) {
        sb.append(".");
        sb.append(array[i]);
      }
      return sb.toString();
    }

    public static FilterCompat.Filter wrapPredicate(FilterPredicate fp) {
      return FilterCompat.get(fp);
    }

    // param device must be only device, without sensor
    private static FilterPredicate getDeviceEqPredicate(String device) {
      String[] deviceElems = device.split("\\.");
      if (ALTER_SCHEMA) {
        // if alternated, only one device
        Binary targetDevice = new Binary.FromStringBinary(device);
        return FilterApi.eq(FilterApi.binaryColumn("deviceID"), targetDevice);
      }

      // all not alternated
      switch (BenchReader.QUERY_DATA_SET) {
        case GeoLife:
        case TDrive:
          Binary targetDevice = new Binary.FromStringBinary(device);
          return FilterApi.eq(FilterApi.binaryColumn("deviceID"), targetDevice);
        case REDD:
          Binary building = new Binary.FromStringBinary(deviceElems[0]);
          Binary meter = new Binary.FromStringBinary(deviceElems[1]);
          return FilterApi.and(
              FilterApi.eq(FilterApi.binaryColumn("building"), building),
              FilterApi.eq(FilterApi.binaryColumn("meter"), meter)
          );
        case TSBS:
          Binary fleet = new Binary.FromStringBinary(deviceElems[0]);
          Binary name = new Binary.FromStringBinary(deviceElems[1]);
          Binary driver = new Binary.FromStringBinary(deviceElems[2]);
          return FilterApi.and(
              FilterApi.eq(FilterApi.binaryColumn("fleet"), fleet),
              FilterApi.and(
                  FilterApi.eq(FilterApi.binaryColumn("name"), name),
                  FilterApi.eq(FilterApi.binaryColumn("driver"), driver)
              )
          );
        case CCS:
        case ZY:
          Binary ent = new Binary.FromStringBinary(deviceElems[0]);
          Binary dev = new Binary.FromStringBinary(deviceElems[1]);
          return FilterApi.and(
              FilterApi.eq(FilterApi.binaryColumn("ent"), ent),
              FilterApi.eq(FilterApi.binaryColumn("dev"), dev)
          );
        default:
          return null;
      }
    }

    public static List<FilterCompat.Filter> singleSeries(ConditionGeneratorV2 cg) {
      List<FilterCompat.Filter> expressions = new ArrayList<>();
      for (String series : cg.singleSeries) {
        String[] nodes = series.split("\\.");
        String device = getDeviceFromNodes(nodes);
        expressions.add(wrapPredicate(getDeviceEqPredicate(device)));
      }
      return expressions;
    }

    public static List<FilterCompat.Filter> alignedDevices(ConditionGeneratorV2 cg) {
      List<FilterCompat.Filter> expressions = new ArrayList<>();
      for (String device : cg.alignedDevices) {
        expressions.add(wrapPredicate(getDeviceEqPredicate(device)));
      }
      return expressions;
    }

    // device equation filter is necessary for all expressions
    public static List<FilterCompat.Filter> timeExpression(ConditionGeneratorV2 cg) {
      List<FilterCompat.Filter> deviceEqs = singleSeries(cg);
      List<FilterCompat.Filter> expressions = new ArrayList<>();
      for (ConditionGeneratorV2.TimeRange tr : cg.timeRanges) {
        String[] nodes = tr.series.split("\\.");
        FilterCompat.Filter tFilter =
            FilterCompat.get(
                FilterApi.and(
                    getDeviceEqPredicate(getDeviceFromNodes(nodes)),
                    FilterApi.and(
                        FilterApi.gtEq(FilterApi.longColumn("timestamp"), tr.t1),
                        FilterApi.ltEq(FilterApi.longColumn("timestamp"), tr.t2)
                    )
                )
            );
        expressions.add(tFilter);
      }
      return expressions;
    }

    public static List<FilterCompat.Filter> valueExpression(ConditionGeneratorV2 cg) {
      List<FilterCompat.Filter> expressions = new ArrayList<>();
      for (ConditionGeneratorV2.DoubleRange dr: cg.doubleRanges) {
        String[] nodes = dr.series.split("\\.");
        String sensorCol = nodes[nodes.length - 1];
        FilterCompat.Filter vFilter =
            FilterCompat.get(
                FilterApi.and(
                    getDeviceEqPredicate(getDeviceFromNodes(nodes)),
                    FilterApi.and(
                        FilterApi.gtEq(FilterApi.doubleColumn(sensorCol), dr.v1),
                        FilterApi.ltEq(FilterApi.doubleColumn(sensorCol), dr.v2)
                    )
                )
            );
        expressions.add(vFilter);
      }
      return expressions;
    }
  }
}
