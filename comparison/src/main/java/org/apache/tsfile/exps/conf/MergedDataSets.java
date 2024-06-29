package org.apache.tsfile.exps.conf;

import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;

import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;

/**
 * Latest class to provide datasets services. 6/28/24
 */
public enum MergedDataSets {
  TDrive("lat", "lon"),
  GeoLife("lat", "lon"),

  // underscore _A means alternative schema
  REDD("elec", "elec"),
  REDD_A("elec", "elec"),

  TSBS("vel", "lat"),
  TSBS_A("vel", "lat");

  MergedDataSets(String fcolumn, String cColumn) {
    this.filteringColumn = fcolumn;
    this.crossColumn = cColumn;
  }

  // to be filtered on for single-column and cross-column query filter
  public String filteringColumn;
  // to be retrieved while not filtered
  public String crossColumn;

  // static final String PRJ_DIR = "F:\\0006DataSets\\"; // @lab
  static final String PRJ_DIR = "E:\\ExpDataSets\\";  // @home

  public static String ARROW_BINS = PRJ_DIR + "Arrows\\";
  public static String CONDITION_DIR = PRJ_DIR + "Conditions\\";
  public static String TARGET_DIR = PRJ_DIR + "Results\\";

  public String getArrowFile() {
    switch (this) {
      case TDrive:
        return ARROW_BINS + "TDrive.bin";
      case GeoLife:
        return ARROW_BINS + "GeoLife.bin";
      case REDD:
        // whatever schema is, the source stay the same
        return ARROW_BINS + "REDD.bin";
      case TSBS:
        return ARROW_BINS + "TSBS.bin";
      default:
    }
    return null;
  }

  public String getConditionBinPath() {
    switch (this) {
      case TDrive:
        return CONDITION_DIR + "TDrive-lat-10.bin";
      case GeoLife:
        return CONDITION_DIR + "GeoLife-lat-10.bin";
      case REDD:
        return CONDITION_DIR + "REDD-elec-10.bin";
      case TSBS:
        return CONDITION_DIR + "TSBS-vel-10.bin";
      default:
    }
    return null;
  }


  // region Parquet-specific

  // for single-column filtering
  public MessageType getParquetSingleColumnSchema() {
    String messageHeader;
    switch (this) {
      case TSBS:
        return parseMessageType("message TSBS { "
            + "required binary name;"
            + "required binary fleet;"
            + "required binary driver;"
            + "required int64 timestamp;"
            + "optional double vel;"
            + "} ");
      case REDD:
        return parseMessageType("message REDD { "
            + "required binary building;"
            + "required binary meter;"
            + "required int64 timestamp;"
            + "required double elec;"
            + "} ");
      case GeoLife:
        messageHeader = "GeoLife";
        break;
      case TDrive:
        messageHeader = "TDrive";
        break;
      case REDD_A:
        messageHeader = "REDD";
        break;
      case TSBS_A:
        messageHeader = "TSBS";
        break;
      default:
        return null;
    }

    return parseMessageType("message " + messageHeader + " { "
        + "required binary deviceID;"
        + "required int64 timestamp;"
        + "required double " + filteringColumn + "; "
        + "} ");
  }

  public FilterCompat.Filter getParquetDeviceComparisonEquation(String device) {
    return FilterCompat.get(getParquetDevicePredicate(device));
  }

  public FilterPredicate getParquetDevicePredicate(String device) {
    return getParquetDevicePredicate(device.split("\\."));
  }

  public FilterPredicate getParquetDevicePredicate(String[] device) {
    switch (this) {
      case TSBS:
        Binary fleet = new Binary.FromStringBinary(device[0]);
        Binary name = new Binary.FromStringBinary(device[1]);
        Binary driver = new Binary.FromStringBinary(device[2]);
        return FilterApi.and(
            FilterApi.eq(FilterApi.binaryColumn("fleet"), fleet),
            FilterApi.and(
                FilterApi.eq(FilterApi.binaryColumn("name"), name),
                FilterApi.eq(FilterApi.binaryColumn("driver"), driver)
            )
        );
      case REDD:
        Binary building = new Binary.FromStringBinary(device[0]);
        Binary meter = new Binary.FromStringBinary(device[1]);
        return FilterApi.and(
            FilterApi.eq(FilterApi.binaryColumn("building"), building),
            FilterApi.eq(FilterApi.binaryColumn("meter"), meter)
        );
      case GeoLife:
      case TDrive:
      case REDD_A:
      case TSBS_A:
        Binary targetDevice = new Binary.FromStringBinary(device[0]);
        return FilterApi.eq(FilterApi.binaryColumn("deviceID"), targetDevice);
      default:
        return null;
    }
  }

  // endregion

}