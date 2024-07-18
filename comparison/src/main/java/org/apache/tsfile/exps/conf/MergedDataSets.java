package org.apache.tsfile.exps.conf;

import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.io.api.Binary;

/**
 * Latest class to provide datasets services. 6/28/24
 */
public enum MergedDataSets {
  TDrive("lat", "lon"),
  GeoLife("lat", "lon"),

  // underscore _A means alternative schema
  REDD("elec", "elec"),
  // REDD_A("elec", "elec"),

  TSBS("vel", "lat"),
  // TSBS_A("vel", "lat"),

  ZY(null, null),
  CCS(null, null);

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

  // added during revision
  public static String NEW_ARW_SRC = PRJ_DIR + "new_arrow_src\\";
  public static String NEW_COND_DIR = PRJ_DIR + "new_cond\\";

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
      case ZY:
        return ARROW_BINS + "ZY.arrow";
      case CCS:
        // original named: 1692611841058-21-1-0.tsfile-2798
        return ARROW_BINS + "CCS.arrow";
      default:
    }
    return null;
  }

  public String getNewArrowFile() {
    return NEW_ARW_SRC + name() + ".arrow";
  }

  public String getNewSupport() {
    return NEW_ARW_SRC + name() + ".sup";
  }

  public String getLegacySupport() {
    return ARROW_BINS + name() + ".sup";
  }

  public String getSupportFile() {
    switch (this) {
      case TDrive:
      case GeoLife:
      case REDD:
      case TSBS:
        return null;
      case ZY:
        return ARROW_BINS + "ZY.sup";
      case CCS:
        return ARROW_BINS + "CCS.sup";
      default:
    }
    return null;
  }

  public String getNewConditionFile() {
    return NEW_COND_DIR + name() + ".cond";
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
      // case REDD_A:
      // case TSBS_A:
        Binary targetDevice = new Binary.FromStringBinary(device[0]);
        return FilterApi.eq(FilterApi.binaryColumn("deviceID"), targetDevice);
      default:
        return null;
    }
  }

  // endregion

}
