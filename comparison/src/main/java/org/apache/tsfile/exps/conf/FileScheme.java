package org.apache.tsfile.exps.conf;

import org.apache.parquet.schema.MessageType;

import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;

public enum FileScheme {
  TsFile,
  Parquet,
  ParquetAS,
  ArrowIPC;


  public MessageType getParquetSchema(MergedDataSets dataSets) {
    switch (dataSets) {
      case TSBS:
        if (this == Parquet) {
          return parseMessageType("message TSBS { "
              + "required binary name;"
              + "required binary fleet;"
              + "required binary driver;"
              + "required int64 timestamp;"
              + "optional double lat;"
              + "optional double lon;"
              + "optional double ele;"
              + "optional double vel;"
              + "} ");
        } else if (this == ParquetAS) {
          return parseMessageType("message TSBS { "
              + "required binary deviceID;"
              + "required int64 timestamp;"
              + "optional double lat;"
              + "optional double lon;"
              + "optional double ele;"
              + "optional double vel;"
              + "} ");
        } else {
          return null;
        }
      case REDD:
        if (this == Parquet) {
          return parseMessageType("message REDD { "
              + "required binary building;"
              + "required binary meter;"
              + "required int64 timestamp;"
              + "required double elec;"
              + "} ");
        } else if (this == ParquetAS){
          return parseMessageType("message REDD { "
              + "required binary deviceID;"
              + "required int64 timestamp;"
              + "required double elec;"
              + "} ");
        } else {
          return null;
        }
      case GeoLife:
        return parseMessageType("message GeoLife { "
            + "required binary deviceID;"
            + "required int64 timestamp;"
            + "required double lon;"
            + "required double lat;"
            + "required double alt;"
            + "} ");
      case TDrive:
        return parseMessageType("message TDrive { "
            + "required binary deviceID;"
            + "required int64 timestamp;"
            + "required double lon;"
            + "required double lat; "
            + "} ");
      default:
        return null;
    }
  }
}
