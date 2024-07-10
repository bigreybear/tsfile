package org.apache.tsfile.exps.vector;

import art.res.util.Pair;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.tsfile.exps.conf.MergedDataSets;
import org.apache.tsfile.exps.loader.ZYLoader;
import org.apache.tsfile.exps.utils.ProgressReporter;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.tsfile.exps.utils.TsFileSequentialConvertor.DICT_ID;

// homemade factory for type-friendly vector operations
public class ArrowVectorHelper {
  public static FieldVector buildNullableVector(Field f,
                                                BufferAllocator a) {
    switch (f.getFieldType().getType().getTypeID()) {
      case Int:
        switch (((Int) f.getType()).getBitWidth()) {
          case 64:
            return new BigIntVector(Field.nullable(f.getName(), f.getType()), a);
          case 32:
            return new IntVector(Field.nullable(f.getName(), f.getType()), a);
          default:
            throw new RuntimeException("Type (wider than 64-bits int) is not supported.");
        }
      case Bool:
        return new BitVector(Field.nullable(f.getName(), f.getType()), a);
      case FloatingPoint:
        switch (((ArrowType.FloatingPoint) f.getType()).getPrecision()) {
          case DOUBLE:
            return new Float8Vector(Field.nullable(f.getName(), f.getType()), a);
          case SINGLE:
            return new Float4Vector(Field.nullable(f.getName(), f.getType()), a);
          case HALF:
            throw new RuntimeException("Type not supported in ArrowFactory.");
        }
      default:
        throw new RuntimeException("Type not supported in ArrowFactory.");
    }
  }

  public static void reAllocTo(ValueVector vector, int tarSiz) {
    while (vector.getValueCapacity() < tarSiz) {
      vector.reAlloc();
    }
  }

  // expand loop in each switch for higher efficiency
  public static void setVectorInBatch(FieldVector sv, FieldVector dv, int si, int di, int span) {
    reAllocTo(dv, di + span);
    switch (sv.getField().getFieldType().getType().getTypeID()) {
      case Int:
        switch (((Int) sv.getField().getType()).getBitWidth()) {
          case 64: {
            BigIntVector tsv = (BigIntVector) sv;
            BigIntVector tbv = (BigIntVector) dv;
            for (int i = 0; i < span; i++) {
              if (tsv.isNull(si + i)) {
                continue;
              }
              tbv.set(di + i, tsv.get(si + i));
            }
            break;
          }
          case 32: {
            IntVector tsv = (IntVector) sv;
            IntVector tbv = (IntVector) dv;
            for (int i = 0; i < span; i++) {
              if (tsv.isNull(si + i)) {
                continue;
              }
              tbv.set(di + i, tsv.get(si + i));
            }
            break;
          }
          default:
            throw new RuntimeException("Type (wider than 64-bits int) is not supported.");
        }
        break;
      case Bool: {
        BitVector tsv = (BitVector) sv;
        BitVector tbv = (BitVector) dv;
        for (int i = 0; i < span; i++) {
          if (tsv.isNull(si + i)) {
            continue;
          }
          tbv.set(di + i, tsv.get(si + i));
        }
        break;
      }
      case FloatingPoint: {
        switch (((ArrowType.FloatingPoint) sv.getField().getType()).getPrecision()) {
          case DOUBLE: {
            Float8Vector tsv = (Float8Vector) sv;
            Float8Vector tbv = (Float8Vector) dv;
            for (int i = 0; i < span; i++) {
              if (tsv.isNull(si + i)) {
                continue;
              }
              tbv.set(di + i, tsv.get(si + i));
            }
            break;
          }
          case SINGLE: {
            Float4Vector tsv = (Float4Vector) sv;
            Float4Vector tbv = (Float4Vector) dv;
            for (int i = 0; i < span; i++) {
              if (tsv.isNull(si + i)) {
                continue;
              }
              tbv.set(di + i, tsv.get(si + i));
            }
            break;
          }
          case HALF:
            throw new RuntimeException("Type not supported in ArrowFactory.");
        }
        break;
      }
      default:
        throw new RuntimeException("Type not supported in ArrowFactory.");
    }
  }

  public static boolean equals(FieldVector v1, FieldVector v2, int s1, int s2, int d) {
    // if (!v1.getName().equals(v2.getName())) {
    //   return false;
    // }

    switch (v1.getField().getFieldType().getType().getTypeID()) {
      case LargeUtf8: {
        LargeVarCharVector tsv = (LargeVarCharVector) v1;
        LargeVarCharVector tbv = (LargeVarCharVector) v2;
        for (int i = 0; i < d; i++) {
          if (tsv.isNull(s1 + i) ^ tbv.isNull(s2 + i)) {
            return false;
          }
          if (tsv.isNull(s1 + i)) {
            continue;
          }

          if (!Arrays.equals(tsv.get(s1 + i), (tbv.get(s2 + i)))) {
            return false;
          }
        }
        return true;
      }
      case Int:
        switch (((Int) v1.getField().getType()).getBitWidth()) {
          case 64: {
            BigIntVector tsv = (BigIntVector) v1;
            BigIntVector tbv = (BigIntVector) v2;
            for (int i = 0; i < d; i++) {
              if (tsv.isNull(s1 + i) ^ tbv.isNull(s2 + i)) {
                return false;
              }
              if (tsv.isNull(s1 + i)) {
                continue;
              }

              if (tsv.get(s1 + i) != tbv.get(s2 + i)) {
                return false;
              }
            }
            return true;
          }
          case 32: {
            IntVector tsv = (IntVector) v1;
            IntVector tbv = (IntVector) v2;
            for (int i = 0; i < d; i++) {
              if (tsv.isNull(s1 + i) ^ tbv.isNull(s2 + i)) {
                return false;
              }
              if (tsv.isNull(s1 + i)) {
                continue;
              }

              if (tsv.get(s1 + i) != tbv.get(s2 + i)) {
                return false;
              }
            }
            return true;
          }
          default:
            System.out.println("Type (wider than 64-bits int) is not supported.");
            throw new RuntimeException("Type (wider than 64-bits int) is not supported.");
        }
      case Bool: {
        BitVector tsv = (BitVector) v1;
        BitVector tbv = (BitVector) v2;
        for (int i = 0; i < d; i++) {
          if (tsv.isNull(s1 + i) ^ tbv.isNull(s2 + i)) {
            return false;
          }
          if (tsv.isNull(s1 + i)) {
            continue;
          }

          if (tsv.get(s1 + i) != tbv.get(s2 + i)) {
            return false;
          }
        }
        return true;
      }
      case FloatingPoint: {
        switch (((ArrowType.FloatingPoint) v1.getField().getType()).getPrecision()) {
          case DOUBLE: {
            Float8Vector tsv = (Float8Vector) v1;
            Float8Vector tbv = (Float8Vector) v2;
            for (int i = 0; i < d; i++) {
              if (tsv.isNull(s1 + i) ^ tbv.isNull(s2 + i)) {
                return false;
              }
              if (tsv.isNull(s1 + i)) {
                continue;
              }

              if (tsv.get(s1 + i) != tbv.get(s2 + i)) {
                return false;
              }
            }
            return true;
          }
          case SINGLE: {
            Float4Vector tsv = (Float4Vector) v1;
            Float4Vector tbv = (Float4Vector) v2;
            for (int i = 0; i < d; i++) {
              if (tsv.isNull(s1 + i) ^ tbv.isNull(s2 + i)) {
                return false;
              }
              if (tsv.isNull(s1 + i)) {
                continue;
              }

              if (tsv.get(s1 + i) != tbv.get(s2 + i)) {
                return false;
              }
            }
            return true;
          }
          case HALF:
            throw new RuntimeException("Type not supported in ArrowFactory.");
        }
      }
      default:
        System.out.println("Type not supported in ArrowFactory.");
        throw new RuntimeException("Type not supported in ArrowFactory.");
    }
  }

  /**
   * Available if each file has a dictionary-coded vector named id.
   */
  public static boolean testArrowFilesEquality(File f1, File f2) throws IOException {
    ProgressReporter pr1 = new ProgressReporter("read blocks for Reader1");
    ProgressReporter pr2 = new ProgressReporter("read blocks for Reader2");
    try (BufferAllocator allocator = new RootAllocator(20 * 1024 * 1024 * 1024L);
         FileInputStream fis1 = new FileInputStream(f1);
         FileInputStream fis2 = new FileInputStream(f2);
         ArrowFileReader r1 = new ArrowFileReader(fis1.getChannel(), allocator, new CommonsCompressionFactory());
         ArrowFileReader r2 = new ArrowFileReader(fis2.getChannel(), allocator, new CommonsCompressionFactory())) {

      Dictionary d1 = r1.getDictionaryVectors().get(DICT_ID);
      Dictionary d2 = r2.getDictionaryVectors().get(DICT_ID);

      pr1.setTotalPoints(r1.getRecordBlocks().size());
      pr2.setTotalPoints(r2.getRecordBlocks().size());

      // rows has been dropped after check
      int l1 = 0, l2 = 0;

      // current block index upon each reader
      int crb1 = 0, crb2 = 0;
      // start offset to check equality on each root
      int s1 = 0, s2 = 0;
      // remaining elements in each root
      int rem1, rem2;

      r1.loadRecordBatch(r1.getRecordBlocks().get(crb1));
      r2.loadRecordBatch(r2.getRecordBlocks().get(crb2));
      for (;;) {

        final VectorSchemaRoot vsr1 = r1.getVectorSchemaRoot();
        final VectorSchemaRoot vsr2 = r2.getVectorSchemaRoot();

        rem1 = vsr1.getRowCount() - s1;
        rem2 = vsr2.getRowCount() - s2;

        int checkLen = Math.min(rem1, rem2);

        final boolean r1DictCoded = !r1.getDictionaryVectors().isEmpty();
        final int _s1 = s1, _s2 = s2;
        List<Pair<String, Boolean>> wr = r1.getVectorSchemaRoot().getFieldVectors().parallelStream().map(v -> {
          // debug
          // System.out.println(String.format("Thread %d processing %s.", Thread.currentThread().getId(), v.getName()));
          if (v.getName().equals("id")) {
            LargeVarCharVector id1;
            if (r1DictCoded) {
              id1 = (LargeVarCharVector) DictionaryEncoder.decode(v, d1);
            } else {
              // for legacy datasets
              if (v.getField().getType().getTypeID() == ArrowType.ArrowTypeID.LargeUtf8) {
                id1 = (LargeVarCharVector) v;
              } else {
                // must be VarCharVector, otherwise throw exception
                VarCharVector vcv = (VarCharVector) v;
                id1 = new LargeVarCharVector("id", allocator);
                for (int i = 0; i < checkLen; i++) {
                  id1.setSafe(i + _s1, vcv.get(i + _s1));
                }
                id1.setValueCount(_s1 + checkLen);
              }
            }
            LargeVarCharVector id2 = (LargeVarCharVector) DictionaryEncoder.decode(vsr2.getVector("id"), d2);
            boolean _res = equals(id1, id2, _s1, _s2, checkLen);
            if (id1 != v) {
              // in case id1 is right reference to the streaming element
              id1.close();
            }
            id2.close();
            return new Pair<>(v.getName(), _res);
          } else {
            return new Pair<>(v.getName(), equals(v, vsr2.getVector(v.getName()), _s1, _s2, checkLen));
          }
        }).filter(p -> !p.right).collect(Collectors.toList());

        if (!wr.isEmpty()) {
          return false;
        }

        s1 += checkLen;
        int oldCrb1 = crb1;
        if (vsr1.getRowCount() == s1) {
          s1 = 0;
          crb1++;
        }

        s2 += checkLen;
        int oldCrb2 = crb2;
        if (vsr2.getRowCount() == s2) {
          s2 = 0;
          crb2++;
        }

        if (r1.getRecordBlocks().size() <= crb1 ^ r2.getRecordBlocks().size() <= crb2) {
          // length not equals: one of two are trying to load nonexistent block
          return false;
        }

        if (r1.getRecordBlocks().size() <= crb1 && r2.getRecordBlocks().size() <= crb2) {
          // all blocks iterated
          break;
        }

        if (crb1 != oldCrb1) {
          l1 += vsr1.getRowCount();
          r1.loadRecordBatch(r1.getRecordBlocks().get(crb1));
          pr1.report(crb1);
        }

        if (crb2 != oldCrb2) {
          l2 += vsr2.getRowCount();
          r2.loadRecordBatch(r2.getRecordBlocks().get(crb2));
          pr2.report(crb2);
        }
      }
      return true;
    }
  }

  public static void main(String[] args) throws IOException {
    boolean b = testArrowFilesEquality(
        new File(MergedDataSets.CCS.getArrowFile()),
        new File("E:\\ExpDataSets\\Results\\CCS_UNCOMPRESSED.arrow")
    );
    System.out.println(b);
  }
}
