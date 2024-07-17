package org.apache.tsfile.exps.updated;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.ValueVector;
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
import org.apache.tsfile.exps.loader.REDDLoaderV2;
import org.apache.tsfile.exps.loader.TSBSLoaderV2;
import org.apache.tsfile.exps.loader.ZYLoaderV2;
import org.apache.tsfile.exps.loader.CCSLoaderV2;
import org.apache.tsfile.exps.loader.AtomicIDDatasetLoaderV2;
import org.apache.tsfile.exps.conf.MergedDataSets;
import org.apache.tsfile.exps.utils.DevSenSupport;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

  /**
   * transfrom OverAllIndex into index of the batches by prefix sum array
   * @param oai overall index
   * @return [index of the batches within list, index within the batch]
   */
  public static int[] getBatchIndex(int oai, int[] psa) {
    if (oai > psa[psa.length - 1] || oai < 0) {
      throw new IndexOutOfBoundsException();
    }

    // only COMP now
    // if (type != ChunkType.COMP) {return null;}

    if (oai < psa[0]) {
      return new int[] {0, oai};
    }
    if (oai >= psa[psa.length - 2]) {
      return new int[] {psa.length - 2, oai - psa[psa.length - 2]};
    }

    int left = 0, right = psa.length - 1, mid = 0;

    // break when oai is between psa[mid-1] and psa[mid]
    while (left < right) {
      mid = (right + left) / 2;
      if (psa[mid] == oai) {
        return new int[] {mid, 0};
      }

      if (oai < psa[mid]) {
        right = mid;
      } else {
        left = mid + 1;
      }
    }

    if (left != right) {
      System.out.println("Strange binary search.");
      System.exit(-1);
    }

    return new int[] {left, oai - psa[left-1]};
  }

  public String getCurDev() {
    return deviceID;
  }

  public LoaderBase() {
    this.allocator = new RootAllocator(16 * 1024 * 1024 * 1024L);
  }

  public boolean hasNext() {
    return iteIdx < ttlRow - 1;
  }

  public boolean next() throws IOException {
    if (iteIdx == ttlRow - 1) {
      return false;
    }
    iteIdx++;
    return true;
  }

  public void initIterator() throws IOException {
    iteIdx = 0;
    ttlRow = idVector.getValueCount();
  }

  // region Getters

  public int getCurrentBatchCursor() {
    return iteIdx;
  }

  public Set<String> getAllDevices() {
    Set<String> res = new HashSet<>();
    for (int i = 0; i < idVector.getValueCount(); i++) {
      if (!idVector.isNull(i)) {
        res.add(new String(idVector.get(i), StandardCharsets.UTF_8));
      }
    }
    return res;
  }

  abstract public Set<String> getRelatedSensors(String did);

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

  /**
   * These two gets-methods should change the internal index.
   */
  public byte[] getID(int i) throws IOException {
    iteIdx = i;
    return idVector.get(i);
  }

  public String getIDString(int i) throws IOException {
    return new String(getID(i), StandardCharsets.UTF_8);
  }

  public long getTS(int i) throws IOException {
    iteIdx = i;
    return timestampVector.get(i);
  }

  public List<FieldVector> getVectors() {
    return root.getFieldVectors();
  }

  public FieldVector getVector(String vecName) {
    return root.getVector(vecName);
  }

  public int reloadAndGetLocalIndex(int glbIdx) throws IOException {
    return glbIdx;
  }

  // endregion

  // reset internal index to the very first element
  public void resetInternalIndex() throws IOException {
    iteIdx = 0;
  }

  public boolean lastOneInBatch() {
    return iteIdx == ttlRow - 1;
  }

  public boolean reachEnd() {return lastOneInBatch();}

  abstract public void updateDeviceID(String fulDev);
  abstract public Group fillGroup(SimpleGroupFactory factory);

  abstract public void fillTablet(Tablet tablet, int rowInTablet);

  // legacy loaders only
  public void initArrays(Tablet tablet) {
    return;
  }
  public void refreshArrays(Tablet tablet) {
    return;
  }

  // designed for revision datasets
  public Tablet refreshTablet(Tablet tablet) {
    return tablet;
  }

  // region Schema & Type

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
      case FloatingPoint: {
        switch (((ArrowType.FloatingPoint)field.getType()).getPrecision()) {
          case DOUBLE:
            builder = Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE);
            break;
          case SINGLE:
            builder = Types.optional(PrimitiveType.PrimitiveTypeName.FLOAT);
            break;
          default:
            throw new RuntimeException("No match datatype.");
        }
        break;
      }
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
      case FloatingPoint: {
        switch (((ArrowType.FloatingPoint)field.getType()).getPrecision()) {
          case DOUBLE:
            return TSDataType.DOUBLE;
          case SINGLE:
            return TSDataType.FLOAT;
          default:
            throw new RuntimeException("No match datatype.");
        }
      }
      default:
        throw new RuntimeException("No match datatype.");
    }
  }

  public List<MeasurementSchema> getSchemaList() {
    throw new UnsupportedOperationException();
    /** legacy
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
     **/
  }

  // endregion

  public static LoaderBase getLoader(MergedDataSets mds) throws IOException, ClassNotFoundException {
    // following constructor with integer parameter is to create an empty object
    switch (mds) {
      case TSBS:
        TSBSLoaderV2 tsbsLoaderV2 = new TSBSLoaderV2(mds);
        return tsbsLoaderV2.deser(mds);
      case REDD:
        REDDLoaderV2 reddLoaderV2 = new REDDLoaderV2(mds);
        return reddLoaderV2.deser(mds);
      case TDrive:
      case GeoLife:
        AtomicIDDatasetLoaderV2 aidLoader = new AtomicIDDatasetLoaderV2(mds);
        return aidLoader.deser(mds);
      case ZY:
        ZYLoaderV2 zyLoaderV2 = new ZYLoaderV2(mds);
        return zyLoaderV2.deser(mds);
      case CCS:
        CCSLoaderV2 ccsLoaderV2 = new CCSLoaderV2(mds);
        return ccsLoaderV2.deser(mds);
      default:
        return null;
    }
  }

  public void close() throws IOException {
    if (idVector != null) {
      idVector.close();
    }

    if (dictionary != null) {
      dictionary.getVector().close();
    }

    if (root != null) {
      root.getFieldVectors().forEach(ValueVector::close);
      root.close();
    }
    if (reader != null) {
      reader.close();
      channel.close();
      fis.close();
    }
    allocator.close();
  }
}
