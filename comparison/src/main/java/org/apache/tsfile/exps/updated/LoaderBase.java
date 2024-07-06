package org.apache.tsfile.exps.updated;

import javafx.scene.control.Tab;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exps.loader.GeoLifeLoader;
import org.apache.tsfile.exps.loader.REDDLoader;
import org.apache.tsfile.exps.loader.TDriveLoader;
import org.apache.tsfile.exps.loader.TSBSLoader;
import org.apache.tsfile.exps.conf.MergedDataSets;
import org.apache.tsfile.exps.loader.ZYLoader;
import org.apache.tsfile.exps.utils.DevSenSupport;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.apache.tsfile.exps.conf.FileScheme.Parquet;
import static org.apache.tsfile.exps.conf.FileScheme.ParquetAS;
import static org.apache.tsfile.exps.updated.BenchWriter.compressorTsFile;
import static org.apache.tsfile.exps.updated.BenchWriter.encodingTsFile;

/**
 * For legacy datasets, i.e., REDD, TSBS, TDrive, and GeoLife, this loader is previously
 * designed for process source data files and rewrite them into uniform arrow file.
 *
 * During revision, it is now responsible for filling data into tablet/group/vectors within
 * competitor file formats.
 */
public abstract class LoaderBase {
  public final BufferAllocator allocator;
  public LargeVarCharVector idVector;
  public BigIntVector timestampVector;

  protected FileInputStream fis = null;
  protected FileChannel channel = null;
  protected ArrowFileReader reader = null;

  // index of the block, index within the block, index all over blocks
  protected int curBlk = 0, curIdx = 0, iteIdx = 0;
  protected int[] psaRow;  // Prefix Sum Array for row numbers in blocks
  protected int ttlRow = 0; // index in current batch, all file

  protected DevSenSupport support;
  protected Dictionary dictionary;
  protected VectorSchemaRoot root;

  protected String deviceID;

  // arrays for legacy loaders
  public static double[] _lats, _lons, _alts, _vels, _elecs, _eles;

  public String getCurDev() {
    return deviceID;
  }

  public LoaderBase() {
    this.allocator = new RootAllocator(16 * 1024 * 1024 * 1024L);
  }

  public boolean hasNext() {
    return iteIdx < ttlRow - 1;
  }

  public void next() throws IOException {
    iteIdx++;
  }

  public void initIterator() throws IOException {
    iteIdx = 0;
    ttlRow = idVector.getValueCount();
  }

  public int getTotalRows() {
    ttlRow = idVector.getValueCount();
    return ttlRow;
  }

  public byte[] getID() {
    return idVector.get(iteIdx);
  }

  public long getTS() {
    return timestampVector.get(iteIdx);
  }

  public byte[] getID(int i) throws IOException {
    return idVector.get(i);
  }

  public long getTS(int i) throws IOException {
    return timestampVector.get(i);
  }

  abstract public void updateDeviceID(String fulDev);
  abstract public Group fillGroup(SimpleGroupFactory factory);

  abstract public void fillTablet(Tablet tablet, int rowInTablet);

  // legacy loaders only
  abstract public void initArrays(Tablet tablet);
  abstract public void refreshArrays(Tablet tablet);

  // designed for revision datasets
  public Tablet refreshTablet(Tablet tablet) {
    return tablet;
  }

  public MessageType getParquetSchema() {
    switch (BenchWriter.mergedDataSets) {
      case TSBS:
        if (BenchWriter.currentScheme == Parquet) {
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
        } else if (BenchWriter.currentScheme == ParquetAS) {
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
        if (BenchWriter.currentScheme == Parquet) {
          return parseMessageType("message REDD { "
              + "required binary building;"
              + "required binary meter;"
              + "required int64 timestamp;"
              + "required double elec;"
              + "} ");
        } else if (BenchWriter.currentScheme == ParquetAS){
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
      case CCS:
      case ZY:
        throw new RuntimeException("Should have been override.");
      default:
        return null;
    }
  }

  // helper for Parquet Type
  protected Type parquetType(Field field) {
    /** Only timestamp is required through this line, while other int64 fields are optional */
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
        throw new RuntimeException("No match datatype.");
    }
    return (Type) builder.named(field.getName());
  }

  // helper for TsFile Type
  protected TSDataType tsfileType(Field field) {
    switch (field.getFieldType().getType().getTypeID()) {
      case Int:
        if (((ArrowType.Int) field.getFieldType().getType()).getBitWidth() == 32) {
          return TSDataType.INT32;
        } else if (((ArrowType.Int) field.getFieldType().getType()).getBitWidth() == 64) {
          if (field.getName().equals("timestamp")) {
            throw new RuntimeException("Not legal vector: timestamp.");
          }
          return TSDataType.INT64;
        }
        throw new RuntimeException("No match datatype: int96?");
      case Bool:
        return TSDataType.BOOLEAN;
      case FloatingPoint:
        return TSDataType.FLOAT;
      default:
        throw new RuntimeException("No match datatype.");
    }
  }

  public List<MeasurementSchema> getSchemaList() {
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
      case CCS:
      case ZY:
        throw new RuntimeException("Should have been override.");
      default:
        return null;
    }
  }

  public static LoaderBase getLoader(MergedDataSets mds) throws IOException {
    // following constructor with integer parameter is to create an empty object
    switch (mds) {
      case TSBS:
        return TSBSLoader.deser(mds);
      case REDD:
        return REDDLoader.deser(mds);
      case TDrive:
        return TDriveLoader.deser(mds);
      case GeoLife:
        return GeoLifeLoader.deser(mds);
      case ZY:
        return ZYLoader.deser(mds);
      default:
        return null;
    }
  }

  public void close() throws IOException {
    allocator.close();
    if (reader != null) {
      reader.close();
      channel.close();
      fis.close();
    }
  }
}
