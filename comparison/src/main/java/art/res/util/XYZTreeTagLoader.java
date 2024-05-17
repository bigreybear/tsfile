package art.res.util;

import ankur.art.ArtNode;
import ankur.art.ArtTree;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.memory.RootAllocator;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Data from XinYuanZhiChu, stable tags of battery box.
 */
// sample:
// tbname,Substation,collectline,cabinet,batterystack,batterycluster,mRID
// "batterybox_meas_luodian87","luodian","1","173","9","18","87"
public class XYZTreeTagLoader {
  public static String srcFile = "xyztree\\boxtag.csv";
  public static String dstFile = "xyztree\\boxtag.arrow";

  public static void main(String[] args) {
    // initArrowBin(srcFile, dstFile);
    ArtTree tree = buildARTFromArrow(dstFile);
    ArtTree.traverse((ArtNode) tree.root, "");
  }

  public static void initArrowBin(String csvFilePath, String arrowFilePath) {
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
         FileReader fileReader = new FileReader(csvFilePath);
         CSVParser parser = CSVParser.parse(fileReader, CSVFormat.DEFAULT);
         FileOutputStream fos = new FileOutputStream(arrowFilePath)) {
      ArrowFileWriter writer = null;
      VectorSchemaRoot root = null;
      List<Field> fields = new ArrayList<>();
      List<FieldVector> vectors = new ArrayList<>();
      boolean initialized = false;
      int rowSize = 0;

      for (CSVRecord record : parser) {
        if (!initialized) {
          // Initialize schema and vectors
          for (int i = 0; i < record.size(); i++) {
            VarCharVector vector = new VarCharVector(record.get(i), allocator);
            fields.add(new Field(record.get(i), FieldType.nullable(vector.getMinorType().getType()), null));
            vectors.add(vector);
            vector.allocateNew();
          }
          Schema schema = new Schema(fields);
          root = new VectorSchemaRoot(fields, vectors);
          // writer = new ArrowFileWriter(root, new DictionaryProvider.MapDictionaryProvider(), Channels.newChannel(fos));
          // writer.start();
          initialized = true;

          // first line only to init
          continue;
        }

        // Fill vectors with data
        for (int i = 0; i < record.size(); i++) {
          String cleanedField = record.get(i).replace("\"", "").trim();
          ((VarCharVector) (vectors.get(i))).setSafe(rowSize, cleanedField.getBytes(StandardCharsets.UTF_8));
        }
        rowSize++;
      }

      final int rs = rowSize;
      vectors.forEach(vector -> vector.setValueCount(rs));
      root.setRowCount(rs);
      writer = new ArrowFileWriter(root, new DictionaryProvider.MapDictionaryProvider(), Channels.newChannel(fos));
      writer.start();
      writer.writeBatch();
      writer.end();
      writer.close();
      vectors.forEach(v -> v.close());

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static ArtTree buildARTFromArrow(String arrowFilePath) {
    try (FileInputStream fis = new FileInputStream(arrowFilePath);
         ArrowFileReader reader = new ArrowFileReader(fis.getChannel(), new RootAllocator(Long.MAX_VALUE))) {

      ArtTree tree = new ArtTree();

      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      while (reader.loadNextBatch()) {
        final int rc = root.getRowCount();
        final int fc = root.getFieldVectors().size();
        final String[] path = new String[fc];
        path[0] = "root";

        for (int i = 0; i < rc; i++) {
          for (int j = 1; j < fc; j++) {
            path[j] = new String(((VarCharVector) root.getVector(j)).get(i), StandardCharsets.UTF_8);
            // System.out.print(new String(res, StandardCharsets.UTF_8));
            // System.out.print(" ");
          }
          // System.out.println("");
          tree.insert(String.join(".", path).getBytes(), (long) i);
        }
      }
      return tree;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }
}
