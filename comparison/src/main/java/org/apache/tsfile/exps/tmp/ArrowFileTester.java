package org.apache.tsfile.exps.tmp;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ArrowFileTester {

  public static void write() throws IOException {
    RootAllocator allocator = new RootAllocator(16 * 1024 * 1024 * 1024L);
    BitVector bitVector = new BitVector("boolean", allocator);
    VarCharVector varCharVector = new VarCharVector("varchar", allocator);
    for (int i = 0; i < 10; i++) {
      bitVector.setSafe(i, i % 2 == 0 ? 0 : 1);
      varCharVector.setSafe(i, ("test" + i).getBytes(StandardCharsets.UTF_8));
    }
    bitVector.setValueCount(10);
    varCharVector.setValueCount(10);

    List<Field> fields = Arrays.asList(bitVector.getField(), varCharVector.getField());
    List<FieldVector> vectors = Arrays.asList(bitVector, varCharVector);
    VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors);

    // 创建临时文件
    File tempFile = new File("test.arrow");
    tempFile.deleteOnExit(); // 确保 JVM 退出时删除文件

    try (FileOutputStream fos = new FileOutputStream(tempFile);
         FileChannel channel = fos.getChannel();
         ArrowFileWriter writer = new ArrowFileWriter(root, null, channel)) {
      // ... do write into the ArrowStreamWriter
      writer.start();
      // write the first batch
      writer.writeBatch();

      // write another four batches.
      for (int i = 0; i < 4; i++) {
        // populate VectorSchemaRoot data and write the second batch
        BitVector childVector1 = (BitVector)root.getVector(0);
        VarCharVector childVector2 = (VarCharVector)root.getVector(1);
        childVector1.reset();
        childVector2.reset();
        // ... do some populate work here, could be different for each batch

        root.setRowCount(i+1);
        childVector1.setValueCount(1);
        childVector2.setValueCount(i+1);
        System.out.println(childVector2.getValueCapacity());
        childVector2.setSafe(i, ("AAAXXX" + i).getBytes(StandardCharsets.UTF_8));

        writer.writeBatch();
      }

      writer.end();
    } catch (Exception e) {
      e.getMessage();
    }
  }

  public static void read() {
    // 假设我们知道临时文件的路径
    String tempFilePath = "test.arrow";
    File tempFile = new File(tempFilePath);

    try (RootAllocator allocator = new RootAllocator()) {
      try (FileInputStream fis = new FileInputStream(tempFile);
           FileChannel channel = fis.getChannel();
           ArrowFileReader reader = new ArrowFileReader(channel, allocator)) {

        System.out.println("Reading Arrow file: " + tempFile.getAbsolutePath());


        // 读取 Schema
        Schema schema = reader.getVectorSchemaRoot().getSchema();
        System.out.println("Schema: " + schema);

        // 遍历所有的 record batches
        while (reader.loadNextBatch()) {
          VectorSchemaRoot root = reader.getVectorSchemaRoot();

          // 假设我们知道第一个字段是 VarCharVector
          Field field = root.getSchema().getFields().get(1);
          VarCharVector vector = (VarCharVector) root.getVector(field.getName());

          System.out.println("Number of rows: " + vector.getValueCount());

          // 打印每一行的数据
          for (int i = 0; i < vector.getValueCount(); i++) {
            if (!vector.isNull(i)) {
              System.out.println("Row " + i + ": " + new String(vector.get(i)));
            } else {
              System.out.println("Row " + i + ": null");
            }
          }
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void main2(String[] args) throws Exception{
    write();
    read();
  }

  public static void main(String[] args)  throws Exception{
    BufferAllocator allocator = new RootAllocator(32 * 1024 * 1024 * 1024L);
    // create provider
    DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();

    try (
        final VarCharVector dictVector = new VarCharVector("dict", allocator);
        final VarCharVector vector = new VarCharVector("vector", allocator);
    ) {
      // create dictionary vector
      dictVector.allocateNewSafe();
      dictVector.setSafe(0, "aa".getBytes());
      dictVector.setSafe(1, "bb".getBytes());
      dictVector.setSafe(2, "cc".getBytes());
      dictVector.setValueCount(3);

      // create dictionary
      Dictionary dictionary =
          new Dictionary(dictVector, new DictionaryEncoding(1L, false, /*indexType=*/null));
      provider.put(dictionary);

      // create original data vector
      vector.allocateNewSafe();
      vector.setSafe(0, "bb".getBytes());
      vector.setSafe(1, "bb".getBytes());
      vector.setSafe(2, "cc".getBytes());
      vector.setSafe(3, "aa".getBytes());
      vector.setValueCount(4);

      // get the encoded vector
      IntVector encodedVector = (IntVector) DictionaryEncoder.encode(vector, dictionary);

      ByteArrayOutputStream out = new ByteArrayOutputStream();

      // create VectorSchemaRoot
      List<Field> fields = Arrays.asList(vector.getField());
      List<FieldVector> vectors = Arrays.asList(vector);
      try (VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors)) {

        // write data
        ArrowStreamWriter writer = new ArrowStreamWriter(root, provider, Channels.newChannel(out));
        writer.start();
        writer.writeBatch();
        writer.end();
      }

      // read data
      try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(out.toByteArray()), allocator)) {
        reader.loadNextBatch();
        VectorSchemaRoot readRoot = reader.getVectorSchemaRoot();
        // get the encoded vector
        // IntVector intVector = (IntVector) readRoot.getVector(0);

        // get dictionaries and decode the vector
        Map<Long, Dictionary> dictionaryMap = reader.getDictionaryVectors();
        // long dictionaryId = intVector.getField().getDictionary().getId();
        // try (VarCharVector varCharVector =
        //          (VarCharVector) DictionaryEncoder.decode(intVector, dictionaryMap.get(dictionaryId))) {
        //   // ... use decoded vector
        // }
        System.out.println("AA");
      }
    }
  }
}
