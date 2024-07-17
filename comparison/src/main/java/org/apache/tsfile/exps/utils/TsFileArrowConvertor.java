package org.apache.tsfile.exps.utils;

import org.apache.arrow.flatbuf.Bool;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.TsFileDeviceIterator;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.utils.Pair;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TsFileArrowConvertor {
  private List<org.apache.arrow.vector.types.pojo.Field> fields;

  private final BufferAllocator allocator;
  private VectorSchemaRoot root;
  private LargeVarCharVector idVector;
  private BigIntVector timestampVector;
  private int rowCount = 0;

  private TsFileSequenceReader sequenceReader;
  private TsFileReader reader;
  private Map<String, List<String>> alignedDevs;  // with sensors
  private Map<String, List<String>> nonAlignedDevs;

  public TsFileArrowConvertor(){
    this.allocator = new RootAllocator(16 * 1024 * 1024 * 1024L);
    fields = new ArrayList<>();
    alignedDevs = new HashMap<>();
    nonAlignedDevs = new HashMap<>();
  }

  // list all measurements for making root schema
  public void preprocess(String path) throws IOException {
    sequenceReader = new TsFileSequenceReader(path);
    reader = new TsFileReader(sequenceReader);

    // collect aligned devices only
    Map<String, List<String>> rs = sequenceReader.getDeviceMeasurementsMap();
    TsFileDeviceIterator iterator = sequenceReader.getAllDevicesIteratorWithIsAligned();
    Pair<String, Boolean> item;
    while (iterator.hasNext()) {
      item = iterator.next();
      if (item.right) {
        alignedDevs.put(
            item.left,
            rs.get(item.left)
        );
      } else {
        nonAlignedDevs.put(
            item.left,
            rs.get(item.left)
        );
      }
    }

    // define columns
    fields.add(
        Field.nullable("id", FieldType.nullable(Types.MinorType.LARGEVARCHAR.getType()).getType())
    );
    fields.add(
        Field.nullable("timestamp", FieldType.nullable(Types.MinorType.BIGINT.getType()).getType())
    );
    Map<String, TSDataType> ms = sequenceReader.getAllMeasurements();
    for (Map.Entry<String, TSDataType> e : ms.entrySet()) {
      fields.add(
          Field.nullable(
              e.getKey(),
              FieldType.nullable(mapTypes(e.getValue()).getType()).getType())
      );
    }

    Schema schema = new Schema(fields);
    root = VectorSchemaRoot.create(schema, allocator);
    idVector = (LargeVarCharVector) root.getVector("id");
    timestampVector = (BigIntVector) root.getVector("timestamp");
  }

  public VectorSchemaRoot processNonAligned() throws IOException {
    List<Path> series = new ArrayList<>();
    QueryExpression expression;
    QueryDataSet res;
    RowRecord row;
    int dataPoints = 0, devNum = 0;
    Stopwatch sw1 = new Stopwatch(), sw2 = new Stopwatch();
    for (Map.Entry<String, List<String>> dev : nonAlignedDevs.entrySet()) {
      series.clear();
      devNum++;
      if (devNum % 10 == 0) {
        System.out.println(String.format("%d %d %d\n", devNum, rowCount, dataPoints));
        System.out.println(String.format("time distribution: %d %d\n", sw1.report(), sw2.report()));
      }
      if (dataPoints > 123459876) {
        return root;
      }

      sw1.start();
      for (String s : dev.getValue()) {
        series.add(new Path(dev.getKey(), s, false));
      }
      sw1.stop();
      expression = QueryExpression.create(series, null);
      res = reader.query(expression);
      sw2.start();
      while (res.hasNext()) {
        row = res.next();
        timestampVector.setSafe(rowCount, row.getTimestamp());
        idVector.setSafe(rowCount, dev.getKey().getBytes(StandardCharsets.UTF_8));
        for (int i = 0; i < dev.getValue().size(); i++) {
          fillVectorWithType(
              dev.getValue().get(i),
              res.getDataTypes().get(i),
              row, i);
        }
        sw2.stop();
        dataPoints += dev.getValue().size();
        rowCount++;
        row.getFields();
      }
    }
    root.setRowCount(rowCount);
    for (FieldVector v : root.getFieldVectors()) {
      v.setValueCount(rowCount);
    }
    return null;
  }

  private void fillVectorWithType(String vname, TSDataType dataType, RowRecord row, int colIdx) {
    if (row.getFields().get(colIdx) == null) {
      return;
    }
    Class<? extends FieldVector> aClass = root.getVector(vname).getClass();
    if (aClass.equals(LargeVarCharVector.class)) {
      ((LargeVarCharVector) root.getVector(vname)).setSafe(rowCount, row.getFields().get(colIdx).toString().getBytes(StandardCharsets.UTF_8));
      return;
    } else if (aClass.equals(Float8Vector.class)) {
      ((Float8Vector) root.getVector(vname)).setSafe(rowCount, row.getFields().get(colIdx).getDoubleV());
      return;
    } else if (aClass.equals(Float8Vector.class)) {
      ((Float8Vector) root.getVector(vname)).setSafe(rowCount, row.getFields().get(colIdx).getFloatV());
      return;
    } else if (aClass.equals(BigIntVector.class)) {
      ((BigIntVector) root.getVector(vname)).setSafe(rowCount, row.getFields().get(colIdx).getIntV());
      return;
    } else if (aClass.equals(BitVector.class)) {
      ((BitVector) root.getVector(vname)).setSafe(rowCount, row.getFields().get(colIdx).getBoolV() ? 1 : 0);
      return;
    }
  }

  private Types.MinorType mapTypes(TSDataType tsDataType) {
    switch (tsDataType) {
      case TEXT:
        return Types.MinorType.LARGEVARCHAR;
      case DOUBLE:
        return Types.MinorType.FLOAT8;
      case FLOAT:
        return Types.MinorType.FLOAT4;
      case INT32:
        return Types.MinorType.INT;
      case INT64:
        return Types.MinorType.BIGINT;
      case BOOLEAN:
        return Types.MinorType.BIT;
      case UNKNOWN:
      default:
        return null;
    }
  }

  public void serialize() throws IOException {
    String path = "F:\\0006DataSets\\Arrows\\ZY.bin";
    try (FileOutputStream fos = new FileOutputStream(path);
         FileChannel channel = fos.getChannel();
         ArrowFileWriter writer = new ArrowFileWriter(root, null, channel)) {
      root.setRowCount(idVector.getValueCount());
      writer.start();
      writer.writeBatch();
      writer.end();
    }
  }

  public static void main(String[] args) throws IOException {
    TsFileArrowConvertor convertor = new TsFileArrowConvertor();
    convertor.preprocess("F:\\0006DataSets\\ZY.tsfile");
    convertor.processNonAligned();
    convertor.serialize();
  }
}
