package org.apache.tsfile.exps.loader.legacy;

import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exps.conf.MergedDataSets;
import org.apache.tsfile.exps.updated.BenchWriter;
import org.apache.tsfile.exps.updated.LoaderBase;
import org.apache.tsfile.exps.utils.DevSenSupport;
import org.apache.tsfile.exps.utils.TsFileSequentialConvertor;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.tsfile.exps.updated.BenchWriter.compressorTsFile;
import static org.apache.tsfile.exps.updated.BenchWriter.encodingTsFile;
import static org.apache.tsfile.exps.utils.TsFileSequentialConvertor.DICT_ID;

public class ZYLoader extends LoaderBase {
  protected Map<String, ValueVector> workingVectors = new HashMap<>();

  // load support file, read through file and collect all length
  public void preprocess(MergedDataSets mds) throws IOException, ClassNotFoundException {
    support = DevSenSupport.deserialize(mds.getLegacySupport());

    psaRow = new int[reader.getRecordBlocks().size()];
    for (int i = 0; i < reader.getRecordBlocks().size(); i++) {
      reader.loadRecordBatch(reader.getRecordBlocks().get(i));
      ttlRow += reader.getVectorSchemaRoot().getRowCount();
      psaRow[i] = ttlRow;
    }
    curBlk = 0;
    reader.loadRecordBatch(reader.getRecordBlocks().get(curBlk));
    root = reader.getVectorSchemaRoot();
    dictionary = reader.getDictionaryVectors().get(DICT_ID);
  }

  public ZYLoader() throws FileNotFoundException {
    this(MergedDataSets.ZY);
  }

  public ZYLoader(MergedDataSets mds) throws FileNotFoundException {
    super();
    fis = new FileInputStream(mds.getArrowFile());
    channel = fis.getChannel();
    reader = new ArrowFileReader(channel, allocator, new CommonsCompressionFactory());
  }

  public ZYLoader(File file) throws FileNotFoundException {
    super();
    fis = new FileInputStream(file);
    channel = fis.getChannel();
    reader = new ArrowFileReader(channel, allocator, new CommonsCompressionFactory());
  }

  public static ZYLoader deser(MergedDataSets mds) throws IOException {
    ZYLoader loader = new ZYLoader(mds);
    try {
      loader.preprocess(mds);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    return loader;
  }

  /**
   * Only used to test correctness, share the support file with the source.
   */
  @Deprecated
  public static ZYLoader deserFromFile(File file) throws IOException {
    ZYLoader loader = new ZYLoader(file);
    try {
      loader.preprocess(MergedDataSets.ZY);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    return loader;
  }

  @Override
  public int getTotalRows() {
    return ttlRow;
  }

  @Override
  public byte[] getID(int idx) throws IOException {
    if (idx == iteIdx) {
      return getID();
    }

    int[] res = getCursor(idx);
    setCursor(res[0], res[1]);
    return getID();
  }

  private int[] getCursor(int idx) {
    if (lastBlockRows() <= idx && idx < psaRow[curBlk]) {
      return new int[] {curBlk, idx - lastBlockRows()};
    } else {
      return LoaderBase.getBatchIndex(idx, psaRow);
    }
  }

  private int lastBlockRows() {
    return curBlk == 0 ? 0 : psaRow[curBlk - 1];
  }

  @Override
  public long getTS(int idx) throws IOException {
    if (idx == iteIdx) {
      return getTS();
    }

    int[] res = getCursor(idx);
    setCursor(res[0], res[1]);
    return getTS();
  }

  @Override
  public int reloadAndGetLocalIndex(int glbIdx) throws IOException {
    int[] res = getCursor(glbIdx);
    setCursor(res[0], res[1]);
    return res[1];
  }

  // set block and ele pointer and load related bytes
  private void setCursor(int blkIdx, int eleIdx) throws IOException {
    if (curBlk != blkIdx) {
      curBlk = blkIdx;
      reader.loadRecordBatch(reader.getRecordBlocks().get(curBlk));
      root = reader.getVectorSchemaRoot();
      updateTsAndIdVector();
    }
    curIdx = eleIdx;
    updateDeviceID(new String(idVector.get(curIdx), StandardCharsets.UTF_8));
  }

  @Override
  public boolean next() throws IOException {
    if (iteIdx == ttlRow - 1) {
      return false;
    }

    iteIdx ++;
    curIdx ++;
    if (iteIdx == psaRow[curBlk]) {
      // current block has run out
      curBlk ++;
      curIdx = 0;
      if (curBlk == reader.getRecordBlocks().size()) {
        throw new RuntimeException("Should not read more blocks.");
      }
      reader.loadRecordBatch(reader.getRecordBlocks().get(curBlk));
      root = reader.getVectorSchemaRoot();
      updateTsAndIdVector();
      String curDev = new String(idVector.get(0), StandardCharsets.UTF_8);
      updateWorkingVectors(curDev);
      deviceID = curDev;
    }
    return true;
  }

  @Override
  public Set<String> getAllDevices() {
    return support.map.keySet();
  }

  @Override
  public int getCurrentBatchCursor() {
    return curIdx;
  }

  @Override
  public Set<String> getRelatedSensors(String did) {
    return support.map.get(did);
  }

  @Override
  public byte[] getID() {
    return idVector.get(curIdx);
  }

  @Override
  public long getTS() {
    return timestampVector.get(curIdx);
  }

  @Override
  public void resetInternalIndex() throws IOException {
    curIdx = curBlk = iteIdx = 0;
    reader.loadRecordBatch(reader.getRecordBlocks().get(0));
    root = reader.getVectorSchemaRoot();
    updateTsAndIdVector();
  }

  @Override
  public boolean lastOneInBatch() {
    return iteIdx == psaRow[curBlk] - 1;
  }

  @Override
  public void initIterator() throws IOException {
    iteIdx = curBlk = curIdx = 0;
    root.getFieldVectors().forEach(ValueVector::reset);
    reader.loadRecordBatch(reader.getRecordBlocks().get(0));
    root = reader.getVectorSchemaRoot();
    updateTsAndIdVector();
  }

  private void updateTsAndIdVector() throws IOException {
    timestampVector = (BigIntVector) root.getVector("timestamp");
    dictionary = reader.getDictionaryVectors().get(DICT_ID);

    if (idVector != null) {
      idVector.close();
    }
    idVector = (LargeVarCharVector) DictionaryEncoder.decode(root.getVector("id"), dictionary);
  }

  // parts of full deviceID
  private String ent = null, dev = null;

  @Override
  public void updateDeviceID(String fulDev) {
    if (deviceID != null && deviceID.equals(fulDev)) {
      return;
    }

    if (BenchWriter.currentScheme != null && BenchWriter.currentScheme.toSplitDeviceID()) {
      String[] nodes = fulDev.split("\\.");
      ent = nodes[0];
      dev = nodes[1];
    }
    updateWorkingVectors(fulDev);
    deviceID = fulDev;
  }

  protected void updateWorkingVectors(String dev) {
    if (dev.equals(deviceID)) {
      return;
    }

    workingVectors.clear();
    for (String s : support.map.get(dev)) {
      workingVectors.put(
          s, root.getVector(s)
      );
    }
  }


  @Override
  public Group fillGroup(SimpleGroupFactory factory) {
    Group g = factory.newGroup();
    for (Map.Entry<String, ValueVector> entry : workingVectors.entrySet()) {
      if (entry.getValue().isNull(curIdx)) {
        continue;
      }

      Object res = entry.getValue().getObject(curIdx);
      switch (entry.getValue().getField().getFieldType().getType().getTypeID()) {
        case Bool:
          g.append(entry.getKey(), (boolean) res);
          break;
        case Int:
          if (((ArrowType.Int) entry.getValue().getField().getFieldType().getType()).getBitWidth() == 32) {
            g.append(entry.getKey(), (int) res);
            break;
          } else if (((ArrowType.Int) entry.getValue().getField().getFieldType().getType()).getBitWidth() == 64) {
            g.append(entry.getKey(), (long) res);
            break;
          } else {
            throw new RuntimeException("Illegal int width during group filling.");
          }
        case FloatingPoint:
          g.append(entry.getKey(), (float) res);
          break;
        default:
          throw new RuntimeException("Illegal type during group filling.");
      }
    }

    g.append("timestamp", timestampVector.get(curIdx));
    if (BenchWriter.currentScheme.toSplitDeviceID()) {
      g.append("ent", ent);
      g.append("dev", dev);
    } else {
      g.append("deviceID", deviceID);
    }
    return g;
  }

  /** A reverse process to {@link TsFileSequentialConvertor#buildVectors()} */
  @Override
  public void fillTablet(Tablet tablet, int rowInTablet) {
    for (Map.Entry<String, ValueVector> entry : workingVectors.entrySet()) {

      if (entry.getValue().isNull(curIdx)) {
        tablet.bitMaps[arrayIdxMapping.get(entry.getKey())].mark(rowInTablet);
        continue;
      }

      switch (arrayTypeMapping.get(entry.getKey())) {
        case FLOAT:
          ((float[]) tablet.values[arrayIdxMapping.get(entry.getKey())])[rowInTablet] =
              ((Float4Vector) entry.getValue()).get(curIdx);
          break;
        case DOUBLE:
          ((double[]) tablet.values[arrayIdxMapping.get(entry.getKey())])[rowInTablet] =
              ((Float8Vector) entry.getValue()).get(curIdx);
          break;
        case BOOLEAN:
          ((boolean[]) tablet.values[arrayIdxMapping.get(entry.getKey())])[rowInTablet] =
              ((BitVector) entry.getValue()).get(curIdx) == 1;
          break;
        case INT64:
          ((long[]) tablet.values[arrayIdxMapping.get(entry.getKey())])[rowInTablet] =
              ((BigIntVector) entry.getValue()).get(curIdx);
          break;
        case INT32:
          ((int[]) tablet.values[arrayIdxMapping.get(entry.getKey())])[rowInTablet] =
              ((IntVector) entry.getValue()).get(curIdx);
          break;
        default:
          throw new RuntimeException("Unexpected type during filling.");
      }
    }
  }

  @Override
  public Tablet refreshTablet(Tablet tablet) {
    Tablet t1 = new Tablet(deviceID, getSchemaList(), BenchWriter.BATCH);
    t1.initBitMaps();
    return t1;
  }

  @Override
  public MessageType getParquetSchema() {
    try (FileInputStream fis = new FileInputStream(BenchWriter.mergedDataSets.getArrowFile());
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
        parquetFields.add(parquetType(field));
      }

      if (BenchWriter.currentScheme.toSplitDeviceID()) {
        // typical ZY path: root.dacoo.ANNJXKJJSIIJXXXXX
        Types.PrimitiveBuilder<?> builder =
            Types.required(PrimitiveType.PrimitiveTypeName.BINARY);
        parquetFields.add((Type) builder.named("ent"));
        parquetFields.add((Type) builder.named("dev"));
      } else {
        Types.PrimitiveBuilder<?> builder =
            Types.required(PrimitiveType.PrimitiveTypeName.BINARY);
        parquetFields.add((Type) builder.named("deviceID"));
      }

      return new MessageType(BenchWriter.mergedDataSets.name(), parquetFields);
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  // metadata for arrays within (TsFile) tablet
  private final Map<String, Integer> arrayIdxMapping = new HashMap<>();
  private final Map<String, TSDataType> arrayTypeMapping = new HashMap<>();

  @Override
  public List<MeasurementSchema> getSchemaList() {
    if (workingVectors.isEmpty()) {
      throw new RuntimeException("No working vectors found.");
    }

    int idx = 0;
    List<MeasurementSchema> msl = new ArrayList<>();
    arrayIdxMapping.clear();
    arrayTypeMapping.clear();
    TSDataType type;
    for (Map.Entry<String, ValueVector> entry : workingVectors.entrySet()) {
      type = tsfileType(entry.getValue().getField());
      msl.add(new MeasurementSchema(
          entry.getKey(),
          type,
          type == TSDataType.BOOLEAN ? TSEncoding.PLAIN : encodingTsFile,  /* boolean incompatible to gorilla */
          compressorTsFile)
      );
      arrayIdxMapping.put(entry.getKey(), idx);
      arrayTypeMapping.put(entry.getKey(), type);
      idx ++;
    }
    return msl;
  }

  public void verifyPtsCount() throws IOException {
    int verify = 0;
    for (ArrowBlock ab : reader.getRecordBlocks()) {
      reader.loadRecordBatch(ab);
      for (ValueVector vv : reader.getVectorSchemaRoot().getFieldVectors()){
        verify += vv.getValueCount() - vv.getNullCount();
      }
    }
    System.out.println(verify);
  }

  public static void main(String[] args) throws IOException {
    ZYLoader loader = deser(MergedDataSets.ZY);
    System.out.println(loader.ttlRow);
  }

  public static void mainx(String[] args) throws IOException, ClassNotFoundException {
    String supPath = MergedDataSets.ARROW_BINS + "ZY.sup";
    DevSenSupport dss = DevSenSupport.deserialize(supPath);
    dss.map.get("A");

    ZYLoader loader = new ZYLoader();
    MergedDataSets mds = MergedDataSets.ZY;
    try (ArrowFileReader reader = new ArrowFileReader(new FileInputStream(
        mds.getArrowFile()).getChannel(), loader.allocator)) {

      // read the 4-th batch
      ArrowBlock block = reader.getRecordBlocks().get(0);
      reader.loadRecordBatch(block);
      VectorSchemaRoot readBatch = reader.getVectorSchemaRoot();
      System.out.println("AAA");
    }
  }
}
