package org.apache.tsfile.exps.conf;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exps.updated.BenchWriter;
import org.apache.tsfile.exps.utils.DevSenSupport;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.apache.tsfile.exps.updated.BenchWriter.compressorTsFile;
import static org.apache.tsfile.exps.updated.BenchWriter.encodingTsFile;

public enum FileScheme {
  TsFile(false),
  Parquet(true),
  ParquetAS(false),
  ArrowIPC(false);

  protected boolean alternated;

  FileScheme(boolean a) {
    alternated = a;
  }

  public boolean toSplitDeviceID() {
    return alternated;
  }

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
      case ZY:
        if (zyMT == null) {
          zyMT = buildParquetSchemaWithArrowSchema(dataSets);
        }
        return zyMT;
      default:
        return null;
    }
  }
  MessageType zyMT = null;

  public MessageType buildParquetSchemaWithArrowSchema(MergedDataSets mds) {

    RootAllocator allocator = new RootAllocator(4 * 1024 * 1024 * 1024L);
    try (FileInputStream fis = new FileInputStream(mds.getArrowFile());
         FileChannel channel = fis.getChannel();
         ArrowFileReader reader = new ArrowFileReader(channel, allocator)) {
      VectorSchemaRoot vectorSchemaRoot = reader.getVectorSchemaRoot();
      // all arrow schema has only one id colum
      Schema arrowSchema = vectorSchemaRoot.getSchema();
      List<Type> parquetFields = new ArrayList<>();

      for (Field field : arrowSchema.getFields()) {
        if (field.getName().equals("id")) {
          continue;
        }
        parquetFields.add(convertField(field));
      }

      if (this.alternated) {
        switch (mds) {
          case ZY:
            // typical ZY path: root.dacoo.ANNJXKJJSIIJXXXXX
            Types.PrimitiveBuilder<?> builder =
                Types.required(PrimitiveType.PrimitiveTypeName.BINARY);
            parquetFields.add((Type) builder.named("ent"));
            parquetFields.add((Type) builder.named("dev"));
            break;
          case CCS:
          default:
            throw new RuntimeException("Should not go here.");
        }
      } else {
        Types.PrimitiveBuilder<?> builder =
            Types.required(PrimitiveType.PrimitiveTypeName.BINARY);
        parquetFields.add((Type) builder.named("deviceID"));
      }

      return new MessageType(mds.name(), parquetFields);
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  /** Only timestamp is required through this line, while other int64 fields are optional */
  private static Type convertField(Field field) {
    Types.PrimitiveBuilder<?> builder = null;
    switch (field.getFieldType().getType().getTypeID()) {
      case Int:
        if (((ArrowType.Int) field.getFieldType().getType()).getBitWidth() == 32) {
          builder = Types.optional(PrimitiveType.PrimitiveTypeName.INT32);
        } else if (((ArrowType.Int) field.getFieldType().getType()).getBitWidth() == 64) {
          builder = field.getName().equals("timestamp")
              ? Types.required(PrimitiveType.PrimitiveTypeName.INT64)
              : Types.optional(PrimitiveType.PrimitiveTypeName.INT64);
        }
        break;
      case Bool:
        builder = Types.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN);
        break;
      case FloatingPoint:
        builder = Types.optional(PrimitiveType.PrimitiveTypeName.FLOAT);
        break;
      default:
        throw new RuntimeException();
    }
    return (Type) builder.named(field.getName());
  }

  public static List<MeasurementSchema> getTsFileSchema(String dev) {
    List<MeasurementSchema> schemas = new ArrayList<>();
    switch (BenchWriter.mergedDataSets) {
      case TSBS:
        schemas.add(new MeasurementSchema("lat", TSDataType.DOUBLE, encodingTsFile, compressorTsFile));
        schemas.add(new MeasurementSchema("lon", TSDataType.DOUBLE, encodingTsFile, compressorTsFile));
        schemas.add(new MeasurementSchema("ele", TSDataType.DOUBLE, encodingTsFile, compressorTsFile));
        schemas.add(new MeasurementSchema("vel", TSDataType.DOUBLE, encodingTsFile, compressorTsFile));
        return schemas;
      case TDrive:
        schemas.add(new MeasurementSchema("lat", TSDataType.DOUBLE, encodingTsFile, compressorTsFile));
        schemas.add(new MeasurementSchema("lon", TSDataType.DOUBLE, encodingTsFile, compressorTsFile));
        return schemas;
      case GeoLife:
        schemas.add(new MeasurementSchema("lat", TSDataType.DOUBLE, encodingTsFile, compressorTsFile));
        schemas.add(new MeasurementSchema("lon", TSDataType.DOUBLE, encodingTsFile, compressorTsFile));
        schemas.add(new MeasurementSchema("alt", TSDataType.DOUBLE, encodingTsFile, compressorTsFile));
        return schemas;
      case REDD:
        schemas.add(new MeasurementSchema("elec", TSDataType.DOUBLE, encodingTsFile, compressorTsFile));
        return schemas;
      case ZY:
        return getTsFileSchemaByDevice(dev);
      default:
        return null;
    }
  }

  private static List<MeasurementSchema> getTsFileSchemaByDevice(String dev) {
    return null;
  }

  public static void main(String[] args) throws IOException, ClassNotFoundException {
    DevSenSupport dss = DevSenSupport.deserialize(MergedDataSets.ZY.getSupportFile());
    FileScheme fs = ParquetAS;
    MessageType mt = fs.buildParquetSchemaWithArrowSchema(MergedDataSets.ZY);
    System.out.println("AAA");
  }
}
