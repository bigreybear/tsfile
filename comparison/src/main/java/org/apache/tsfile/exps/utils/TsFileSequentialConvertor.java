/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tsfile.exps.utils;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exps.ConditionGenerator;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.header.ChunkGroupHeader;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.reader.page.PageReader;
import org.apache.tsfile.read.reader.page.TimePageReader;
import org.apache.tsfile.read.reader.page.ValuePageReader;
import org.apache.tsfile.utils.TsPrimitiveType;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *  This tool is used to read TsFile sequentially, including nonAligned or aligned timeseries.
 *
 *  <p>
 *
 *  Enhanced to convert TsFile sequentially rather than interactively, i.e., with queries.
 *
 *  <p>
 *
 *  The motivation is that, TsFile is designed to be a multi-dimension file format rather than
 *  an ordinary tabular (2-dimension) format. The conversion may increase the universality of
 *  the content.
 *
 *  <p>
 *
 *  The goal is to convert into an ordinary tabular format, like Arrow. Specifically, this
 *  would integrate all deviceID into a single column, and store all time-series from measurement
 *  with identical name into a single column.
 *  The final table will have one column for each unique sensor name, plus two additional columns:
 *  one for timestamp and another for deviceID.
 *
 *  <p>
 *
 *  DEFICIENCY: only int, big int, float and boolean columns are available. <br>
 *  TODO: add options, for either HIGH_FIDELITY or ONLY_NUMERIC. The former will keep diverging type
 *  columns as TEXT and store all data (however obviously inefficient), the latter will only keep
 *  numeric columns.
 *
 *  @authored by Zhou Peichen originally, modified by Zhao Xin
 * */
public class TsFileSequentialConvertor {
  // configurations.
  public static final String srcPath = "F:\\0006DataSets\\ZY.tsfile"; // path to read TsFile
  public static final String dstPath = "F:\\0006DataSets\\ZY.arrow"; // path to write ArrowIPC
  public static final String supPath = "F:\\0006DataSets\\ZY.sup";  // path to supporting file
  private static final int BATCH_ROW = 100_000;
  private static final boolean ONLY_PART = true;
  private static final int PART_START = -1, PART_END = 240000000;
  // private static final boolean HIGH_FIDELITY = false;  // to develop

  private static final long[] WORKING_RANGE = new long[] {-1, Long.MAX_VALUE};

  private static boolean printDetail = false;
  public static final String POINT_IN_PAGE = "\t\tpoints in the page: ";

  // basical fields for Arrow
  private final BufferAllocator allocator;
  private VectorSchemaRoot root;
  private int totalRowCount = 0, locRowIdx = 0;
  private long ptsCount = 0;

  private final LargeVarCharVector idVector;
  private IntVector idVectorEncoded;
  private Dictionary idDict;
  private final BigIntVector timestampVector;

  private FileOutputStream fos;
  private FileChannel channel;
  private ArrowFileWriter writer;

  // each array per vector type
  private final Map<String, LargeVarCharVector> largeVarCharVectors = new HashMap<>();
  private final Map<String, Float8Vector> float8Vectors = new HashMap<>();
  private final Map<String, Float4Vector> float4Vectors = new HashMap<>();
  private final Map<String, BigIntVector> bigIntVectors = new HashMap<>();
  private final Map<String, IntVector> intVectors = new HashMap<>();
  private final Map<String, BitVector> bitVectors = new HashMap<>();
  private final Map<String, TSDataType> vectorNameType = new HashMap<>(); // collect vector names across types

  // collect data during sequential traversal
  private final List<ChunkData> collectedChunks = new ArrayList<>();
  private boolean effectiveAligned = true;
  private String processingDev = null;
  private final Set<String> devSets = new HashSet<>();


  // collect deserialized chunk data for conversion before next chunk group
  private class ChunkData {
    ChunkType type;
    TSDataType dataType;
    List<long[]> timeBatch;
    List<TsPrimitiveType[]> valueBatch;
    List<BatchData> compBatch;
    String name;
    BitSet bs;
    int[] psa;  // prefix sum array
    int capacity;

    // time
    ChunkData(List<long[]> tb) {
      type = ChunkType.TIME;
      timeBatch = tb;
    }

    // val
    ChunkData(ChunkHeader header, TsPrimitiveType[] tpt) {
      type = ChunkType.VALUE;
      name = header.getMeasurementID();
      dataType = header.getDataType();
      valueBatch = new ArrayList<>();
      valueBatch.add(tpt);
    }

    // comp
    ChunkData(ChunkHeader header, BatchData bd) {
      type = ChunkType.COMP;
      name = header.getMeasurementID();
      dataType = header.getDataType();
      compBatch = new ArrayList<>();
      compBatch.add(bd);
    }

    /**
     * transfrom OverAllIndex into index of the batches by prefix sum array
     * @param oai overall index
     * @return [index of the batches within list, index within the batch]
     */
    int[] getBatchIndex(int oai) {
      if (oai > psa[psa.length - 1] || oai < 0) {
        throw new IndexOutOfBoundsException();
      }

      // only COMP now
      if (type != ChunkType.COMP) {return null;}

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
      return new int[] {mid, oai - psa[mid-1]};
    }
  }

  private enum ChunkType {
    TIME,
    VALUE,
    COMP;
  }

  /* Record mappings between devices and sensors for sparse tables. */
  public static class DevSenSupport implements Serializable {
    private static final long serialVersionUID = 6395645743397020735L;
    public final Map<String, Set<String>> map = new HashMap<>();

    private void add(String d, String s) {
      if (!map.containsKey(d)) {
        map.put(d, new HashSet<>());
      }
      map.get(d).add(s);
    }

    private void addByChunks(String d, List<ChunkData> chunkData) {
      chunkData.forEach(c -> add(d, c.name));
    }

    public static void serialize(DevSenSupport dss, String path) throws IOException {
      ObjectOutputStream oos = new ObjectOutputStream(Files.newOutputStream(Paths.get(path)));
      oos.writeObject(dss);
      oos.close();
    }

    public static DevSenSupport deserialize(String path) throws IOException, ClassNotFoundException{
      try (ObjectInputStream ois = new ObjectInputStream(Files.newInputStream(Paths.get(path)))){
        return (DevSenSupport) ois.readObject();
      }
    }
  }

  public TsFileSequentialConvertor(BufferAllocator all) {
    this.allocator = all;
    // this.idVectorEncoded = new IntVector(Field.nullable("id", Types.MinorType.INT.getType()), allocator);
    this.idVector = new LargeVarCharVector(Field.nullable("id", Types.MinorType.LARGEVARCHAR.getType()), allocator);
    idVector.allocateNew(BATCH_ROW);
    this.timestampVector = new BigIntVector(Field.nullable("timestamp", Types.MinorType.BIGINT.getType()), allocator);
    timestampVector.allocateNew(BATCH_ROW);
  }

  public TsFileSequentialConvertor() {
    this(new RootAllocator(32 * 1024 * 1024 * 1024L));
  }

  // region Utils

  // always truncate first dot
  private static final String truncateDeviceID(String oid) {
    return oid.substring(oid.indexOf(".") + 1);
  }

  // test only
  private String printBisSets() {
    int cnt = 0;
    StringBuilder builder = new StringBuilder();
    for (ChunkData cdata : collectedChunks) {
      cnt++;
      if (cdata.bs != null) {
        builder.append(cnt);
        builder.append(String.format(" %s\n", cdata.bs));
      }
    }
    System.out.println(builder);
    return builder.toString();
  }

  // helper
  private static float getFloatVal(TSDataType type, BatchData bd) {
    switch (type) {
      case FLOAT:
        return bd.getFloat();
      case INT32:
        return bd.getInt();
      case BOOLEAN:
        return bd.getBoolean() == true ? 1.0f : 0.0f;
      case INT64:
        return bd.getLong();
      case DOUBLE:
        return (float) bd.getDouble();
      case TEXT:
      case UNKNOWN:
      case VECTOR:
      default:
        throw new UnsupportedOperationException();
    }
  }

  // helper
  private static int getIntVal(TSDataType type, BatchData bd) {
    switch (type) {
      case BOOLEAN:
        return bd.getBoolean() == true ? 1 : 0;
      case INT64:
        return (int) bd.getLong();
      case INT32:
        return bd.getInt();
      case DOUBLE:
      case FLOAT:
      case TEXT:
      case UNKNOWN:
      case VECTOR:
      default:
        throw new UnsupportedOperationException();
    }
  }

  private void reportChunkTypes() {
    Map<TSDataType, AtomicInteger> res = new HashMap<>();
    for (Map.Entry<String, TSDataType> entry : vectorNameType.entrySet()) {
      if (!res.containsKey(entry.getValue())) {
        res.put(entry.getValue(), new AtomicInteger(0));
      }

      res.get(entry.getValue()).incrementAndGet();
    }

    System.out.println(res);
  }

  private void close() throws IOException {
    closeWithoutAllocator();
    allocator.close();
  }

  protected void closeWithoutAllocator() throws IOException{
    writeCurrentBatch();
    writer.end();
    writer.close();
    System.out.println("File completed.");

    idVector.close();
    root.getFieldVectors().forEach(ValueVector::close);
    root.clear();
    root.close();
    channel.close();
    fos.close();
  }

  private boolean inLegalRange(long curPos) {
    return !ONLY_PART || ((WORKING_RANGE[0] <= curPos) && ( curPos <= WORKING_RANGE[1]));
  }

  // endregion

  // region Vector Manipulator

  /**
   * The key to merge multiple unaligned chunks is, to merge the timestamps.
   * Furthermore, use the bitmap to indicate which element is absent for individual vectors.
   * @return merged and sorted timestamp vector
   */
  private long[] mergeTimestampArrays() {
    TreeSet<Long> mergedTimestamps = new TreeSet<>();

    for (ChunkData cdata : collectedChunks) {
      if (cdata.type != ChunkType.COMP) {
        throw new RuntimeException("Wrong chunk type.");
      }

      int sum = 0, i = 0;
      cdata.psa = new int[cdata.compBatch.size()];
      for (BatchData bdata : cdata.compBatch) {
        // build prefix sum index
        sum += bdata.count;
        cdata.psa[i++] = sum;

        // Note: dangerous usage
        for (long[] tsv : bdata.timeRet) {
          for (long v : tsv) {
            if (v == 0) {
              // skip all padding 0s
              break;
            }
            mergedTimestamps.add(v);
          }
        }
      }
    }

    final long[] result = mergedTimestamps.stream().mapToLong(Long::longValue).toArray();

    final int resLen = result.length;
    for (ChunkData cdata : collectedChunks) {
      // for each chunk, check batches with the result, mark absence on bitset
      cdata.bs = new BitSet(resLen);
      int resIdx = 0;

      for (BatchData bd : cdata.compBatch) {
        long cts; // current time index
        bd.resetBatchData();
        while (bd.hasCurrent() && resIdx < resLen) {
          cts = bd.currentTime();
          if (result[resIdx] == cts) {
            resIdx ++;
            bd.next();
            continue;
          }

          if (result[resIdx] > cts) {
            bd.next();
          } else {
            cdata.bs.set(resIdx);
             if (effectiveAligned) {
               effectiveAligned = false;
             }
            resIdx++;
          }
        }
      }
      if (resIdx < resLen) {
        cdata.bs.set(resIdx, resLen);
      }
    }
    return result;
  }

  private void buildVectors() {
    for (Map.Entry<String, TSDataType> entry : vectorNameType.entrySet()) {
      switch (entry.getValue()) {
        case DOUBLE:
        case FLOAT:
          float4Vectors.put(
              entry.getKey(),
              new Float4Vector(Field.nullable(entry.getKey(), Types.MinorType.FLOAT4.getType()), allocator)
          );
          break;
        case INT32:
          intVectors.put(
              entry.getKey(),
              new IntVector(Field.nullable(entry.getKey(), Types.MinorType.INT.getType()), allocator)
          );
          break;
        case BOOLEAN:
          bitVectors.put(
              entry.getKey(),
              new BitVector(Field.nullable(entry.getKey(), Types.MinorType.BIT.getType()), allocator)
          );
          break;
        case INT64:
          bigIntVectors.put(
              entry.getKey(),
              new BigIntVector(Field.nullable(entry.getKey(), Types.MinorType.BIGINT.getType()), allocator)
          );
          break;
        case VECTOR:
        case TEXT:
        case UNKNOWN:
        default:
          System.out.println(String.format("Unknown type chunk: %s.%s",
              processingDev,
              entry.getKey()));
      }
    }
  }

  static private boolean checkType(TSDataType dt1, TSDataType dt2, TSDataType dst) {
    return dt1 == dst || dt2 == dst;
  }

  private void unifyChunkDataType() {
    for (ChunkData cdata : collectedChunks) {
      if (cdata.type == ChunkType.TIME) {
        continue;
      }

      if (vectorNameType.containsKey(cdata.name)) {
        if (vectorNameType.get(cdata.name) == cdata.dataType) {
          continue;
        }

        TSDataType dt1 = vectorNameType.get(cdata.name), dt2 = cdata.dataType;

        if (checkType(dt1, dt2, TSDataType.FLOAT)  || checkType(dt1, dt2, TSDataType.DOUBLE)) {
          vectorNameType.put(cdata.name, TSDataType.FLOAT);
        } else if (checkType(dt1, dt2, TSDataType.INT32) || checkType(dt1, dt2, TSDataType.INT64)) {
          vectorNameType.put(cdata.name, TSDataType.INT32);
        } else if (checkType(dt1, dt2, TSDataType.BOOLEAN)) {
          // boolean vs text
          vectorNameType.put(cdata.name, TSDataType.BOOLEAN);
        }
      } else {
        vectorNameType.put(cdata.name, cdata.dataType);
      }

    }
  }

  private void removeVector(TSDataType type, String name) {
    switch (type) {
      case TEXT:
        largeVarCharVectors.remove(name);
      case DOUBLE:
        float8Vectors.remove(name);
      case INT64:
        bigIntVectors.remove(name);
      case BOOLEAN:
        bitVectors.remove(name);
      case INT32:
        intVectors.remove(name);
      case FLOAT:
        float4Vectors.remove(name);
      case VECTOR:
      case UNKNOWN:
      default:
        throw new UnsupportedOperationException();
    }
  }

  // endregion

  /**
   * Scan all chunks, build the mapping between chunk name and datatype, and create the
   * corresponding vectors.
   * For typical time series data, all text type would be dropped, and different type chunks
   * with same name would be cast to FLOAT.
   *
   * @throws IOException
   */
  public void preprocess() throws IOException{
    DevSenSupport support = new DevSenSupport();

    // adjust working range
    if (ONLY_PART) {
      WORKING_RANGE[0] = PART_START;
      WORKING_RANGE[1] = PART_END;
    }

    String filename = srcPath;
    try (TsFileSequenceReader reader = new TsFileSequenceReader(filename)) {

      float lastProgressReported = 0.0f, progress = 0.0f;
      // Note(zx) start of the file
      reader.position((long) TSFileConfig.MAGIC_STRING.getBytes().length + 1);
      List<long[]> timeBatch = new ArrayList<>();
      int pageIndex = 0;
      byte marker;
      while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
        switch (marker) {
          case MetaMarker.CHUNK_HEADER:
          case MetaMarker.TIME_CHUNK_HEADER:
          case MetaMarker.VALUE_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_TIME_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_VALUE_CHUNK_HEADER:
            ChunkHeader header = reader.readChunkHeader(marker);
            if (header.getDataSize() == 0) {
              // empty value chunk
              break;
            }
            Decoder defaultTimeDecoder =
                Decoder.getDecoderByType(
                    TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
                    TSDataType.INT64);
            Decoder valueDecoder =
                Decoder.getDecoderByType(header.getEncodingType(), header.getDataType());
            int dataSize = header.getDataSize();
            pageIndex = 0;
            if (header.getDataType() == TSDataType.VECTOR) {
              timeBatch.clear();
            }

            ChunkData cdata = null;  // Note(zx) to collect chunks
            while (dataSize > 0) {
              valueDecoder.reset();
              PageHeader pageHeader =
                  reader.readPageHeader(
                      header.getDataType(),
                      (header.getChunkType() & 0x3F) == MetaMarker.CHUNK_HEADER);
              ByteBuffer pageData = reader.readPage(pageHeader, header.getCompressionType());

              // Time Chunk
              if ((header.getChunkType() & TsFileConstant.TIME_COLUMN_MASK)
                  == TsFileConstant.TIME_COLUMN_MASK) {
                TimePageReader timePageReader =
                    new TimePageReader(pageHeader, pageData, defaultTimeDecoder);
                timeBatch.add(timePageReader.getNextTimeBatch());

                // Value Chunk
              } else if ((header.getChunkType() & TsFileConstant.VALUE_COLUMN_MASK)
                  == TsFileConstant.VALUE_COLUMN_MASK) {
                ValuePageReader valuePageReader =
                    new ValuePageReader(pageHeader, pageData, header.getDataType(), valueDecoder);
                TsPrimitiveType[] valueBatch =
                    valuePageReader.nextValueBatch(timeBatch.get(pageIndex));

                if (pageIndex == 0) {
                  cdata = new ChunkData(header, valueBatch);
                } else {
                  cdata.valueBatch.add(valueBatch);
                }

                // NonAligned Chunk
              } else {
                PageReader pageReader =
                    new PageReader(
                        pageData, header.getDataType(), valueDecoder, defaultTimeDecoder);
                BatchData batchData = pageReader.getAllSatisfiedPageData();

                if (pageIndex == 0) {
                  cdata = new ChunkData(header, batchData);
                } else {
                  cdata.compBatch.add(batchData);
                }
              }
              pageIndex++;
              dataSize -= pageHeader.getSerializedPageSize();
            }

            // summary one chunk which has finished
            this.collectedChunks.add(cdata);
            break;
          case MetaMarker.CHUNK_GROUP_HEADER:

            // handle chunks from last ChunkGroup
            if (!collectedChunks.isEmpty() && inLegalRange(reader.position())) {
              support.addByChunks(processingDev, collectedChunks);
              unifyChunkDataType();
            }

            progress = reader.position() * 1.0f / (reader.fileSize() - reader.getAllMetadataSize());
            if (progress - lastProgressReported > 0.1) {
              lastProgressReported = progress;
              String time = DateTimeFormatter.ofPattern("HH:mm:ss").format(java.time.LocalDateTime.now());
              System.out.println(String.format(
                  "prepocessing progress: %f, at time: %s",
                  progress, time));
            }
            // reader.readChunkGroupHeader();
            processingDev = truncateDeviceID(reader.readChunkGroupHeader().getDeviceID());
            devSets.add(processingDev);
            // all chunks cleared
            collectedChunks.clear();
            break;
          case MetaMarker.OPERATION_INDEX_RANGE:
            reader.readPlanIndex();
            System.out.println("minPlanIndex: " + reader.getMinPlanIndex());
            System.out.println("maxPlanIndex: " + reader.getMaxPlanIndex());
            break;
          default:
            MetaMarker.handleUnexpectedMarker(marker);
        }
      }
      DevSenSupport.serialize(support, supPath);
      buildVectors();
    }
  }

  /**
   * Note(zx)
   * Sequentially read TsFile, convert ChunkGroups once at a time.
   * For each ChunkGroup, all chunks are deserialized and buffered, and then turned into
   * vectors all at once.
   * @throws IOException
   */
  public void processFile() throws Exception {
    collectedChunks.clear();

    try (TsFileSequenceReader reader = new TsFileSequenceReader(srcPath)) {

      // Sequential reading of one ChunkGroup now follows this order:
      // first the CHUNK_GROUP_HEADER, then SeriesChunks (headers and data) in one ChunkGroup
      // Because we do not know how many chunks a ChunkGroup may have, we should read one byte (the
      // marker) ahead and judge accordingly.

      final long fullDataSize = reader.fileSize() - reader.getAllMetadataSize();
      float lastProgressReported = 0.0f, progress = 0.0f;
      int alignedCG = 0, unalignedCG = 0, effectiveUnalignedCG = 0;

      // Note(zx) start of the file
      reader.position((long) TSFileConfig.MAGIC_STRING.getBytes().length + 1);
      List<long[]> timeBatch = new ArrayList<>();
      int pageIndex = 0;
      byte marker;
      boolean finishInAdvece = false;
      while (((marker = reader.readMarker()) != MetaMarker.SEPARATOR) && !finishInAdvece ) {
        switch (marker) {
          case MetaMarker.CHUNK_HEADER:
          case MetaMarker.TIME_CHUNK_HEADER:
          case MetaMarker.VALUE_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_TIME_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_VALUE_CHUNK_HEADER:
            ChunkHeader header = reader.readChunkHeader(marker);
            if (header.getDataSize() == 0) {
              // empty value chunk
              System.out.println("\t-- Empty Chunk ");
              break;
            }
            Decoder defaultTimeDecoder =
                Decoder.getDecoderByType(
                    TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
                    TSDataType.INT64);
            Decoder valueDecoder =
                Decoder.getDecoderByType(header.getEncodingType(), header.getDataType());
            int dataSize = header.getDataSize();
            pageIndex = 0;
            if (header.getDataType() == TSDataType.VECTOR) {
              timeBatch.clear();
            }

            ChunkData cdata = null;  // Note(zx) to collect chunks
            while (dataSize > 0) {
              valueDecoder.reset();
              PageHeader pageHeader =
                  reader.readPageHeader(
                      header.getDataType(),
                      (header.getChunkType() & 0x3F) == MetaMarker.CHUNK_HEADER);
              ByteBuffer pageData = reader.readPage(pageHeader, header.getCompressionType());

              // Time Chunk
              if ((header.getChunkType() & TsFileConstant.TIME_COLUMN_MASK)
                  == TsFileConstant.TIME_COLUMN_MASK) {
                TimePageReader timePageReader =
                    new TimePageReader(pageHeader, pageData, defaultTimeDecoder);
                timeBatch.add(timePageReader.getNextTimeBatch());

                if (pageIndex == 0) {
                  cdata = new ChunkData(timeBatch);
                }

                // Value Chunk
              } else if ((header.getChunkType() & TsFileConstant.VALUE_COLUMN_MASK)
                  == TsFileConstant.VALUE_COLUMN_MASK) {
                ValuePageReader valuePageReader =
                    new ValuePageReader(pageHeader, pageData, header.getDataType(), valueDecoder);
                TsPrimitiveType[] valueBatch =
                    valuePageReader.nextValueBatch(timeBatch.get(pageIndex));

                // only non-text chunks recorded
                if (header.getDataType() != TSDataType.TEXT) {
                  if (pageIndex == 0) {
                    cdata = new ChunkData(header, valueBatch);
                  } else {
                    cdata.valueBatch.add(valueBatch);
                  }
                }

                // NonAligned Chunk
              } else {
                PageReader pageReader =
                    new PageReader(
                        pageData, header.getDataType(), valueDecoder, defaultTimeDecoder);
                BatchData batchData = pageReader.getAllSatisfiedPageData();

                if (header.getDataType() != TSDataType.TEXT) {
                  if (pageIndex == 0) {
                    cdata = new ChunkData(header, batchData);
                  } else {
                    cdata.compBatch.add(batchData);
                  }
                }
              }
              pageIndex++;
              dataSize -= pageHeader.getSerializedPageSize();
            }

            // summary one chunk has finished
            if (cdata != null) {
              this.collectedChunks.add(cdata);
            }
            break;
          case MetaMarker.CHUNK_GROUP_HEADER:
            // handle chunks from last ChunkGroup
            if (!collectedChunks.isEmpty() && inLegalRange(reader.position())) {

              // process last ChunkGroup
              if (collectedChunks.get(0).type == ChunkType.COMP) {
                unalignedCG++;
                // it is an unaligned ChunkGroup
                effectiveAligned = true;
                long[] mergedTs = mergeTimestampArrays();
                fillUnalignedChunksIntoVectors(mergedTs);
                if (!effectiveAligned) {
                } else {
                  effectiveUnalignedCG ++;
                }
              } else {
                fillAlignedChunksIntoVectors();
                alignedCG ++;
              }
            }

            // report progress
            progress = reader.position() * 1.0f / fullDataSize;
            if (progress - lastProgressReported > 0.05) {
              lastProgressReported = progress;
              String time = DateTimeFormatter.ofPattern("HH:mm:ss").format(java.time.LocalDateTime.now());
              System.out.println(String.format(
                  "progress: %f, Aligned cg: %s, unaligned cg: %s, effective aligned cg: %s, " +
                      "total rows: %d, batch rows: %d, pts: %s, time at: %s",
                  progress, alignedCG, unalignedCG, effectiveUnalignedCG,
                  totalRowCount, locRowIdx, ptsCount, time));
            }

            ChunkGroupHeader chunkGroupHeader = reader.readChunkGroupHeader();
            processingDev = truncateDeviceID(chunkGroupHeader.getDeviceID());
            // all chunks cleared
            collectedChunks.clear();

            if (reader.position() > WORKING_RANGE[1]) {
              System.out.println("Finish scanning in advance.");
              finishInAdvece = true;
            }

            break;
          case MetaMarker.OPERATION_INDEX_RANGE:
            reader.readPlanIndex();
            System.out.println("minPlanIndex: " + reader.getMinPlanIndex());
            System.out.println("maxPlanIndex: " + reader.getMaxPlanIndex());
            break;
          default:
            MetaMarker.handleUnexpectedMarker(marker);
        }
      }
    }
  }

  // region Fill and Write

  public static void reAllocTo(ValueVector vector, int tarSiz) {
    while (vector.getValueCapacity() < tarSiz) {
      vector.reAlloc();
    }
  }

  /**
   * fill in ts and id
   * find chunk vector one by one
   * check length or extend
   * fill in
   * next chunk and its vector
   */
  private void fillUnalignedChunksIntoVectors(long[] mts) throws Exception{

    if (this.locRowIdx >= BATCH_ROW) {
      writeCurrentBatch();
    }

    final int soi = this.locRowIdx; // Start Offset on vectors for this Run
    final int rln = mts.length;    // Result Length
    final int vti = rln + soi; // Vector Target Index
    this.totalRowCount += rln;
    this.locRowIdx = vti;


    byte[] devByt = processingDev.getBytes(StandardCharsets.UTF_8);
    // check and fill vectors of ts and id
    reAllocTo(timestampVector, vti);
    reAllocTo(idVector, vti);
    for (int i = 0; i < rln; i++) {
      timestampVector.set(soi + i, mts[i]);
      idVector.setSafe(soi + i, devByt);
    }

    ptsCount += 2 * rln;

    for (ChunkData cdata : collectedChunks) {
      switch (vectorNameType.get(cdata.name)) {
        case FLOAT: {
          Float4Vector vector = float4Vectors.get(cdata.name);
          reAllocTo(vector, vti);
          int idx = 0;
          for (BatchData bd : cdata.compBatch) {
            bd.resetBatchData();
            while (bd.hasCurrent()) {
              if (cdata.bs.get(idx)) {
                idx++;
                continue;
              }
              vector.set(idx + soi, getFloatVal(cdata.dataType, bd));
              bd.next();
              idx++;
              ptsCount++;
            }
          }
          if (idx != rln) {
            System.out.println("Some math wrong");
          }
          break;
        }
        case INT32: {
          IntVector vector = intVectors.get(cdata.name);
          reAllocTo(vector, vti);
          int idx = 0;
          for (BatchData bd : cdata.compBatch) {
            bd.resetBatchData();
            while (bd.hasCurrent()) {
              if (cdata.bs.get(idx)) {
                idx++;
                continue;
              }
              vector.set(idx + soi, getIntVal(cdata.dataType, bd));
              bd.next();
              idx++;
              ptsCount++;
            }
          }
          if (idx != rln) {
            System.out.println("Some math wrong");
          }
          break;
        }
        case BOOLEAN: {
          BitVector vector = bitVectors.get(cdata.name);
          reAllocTo(vector, vti);
          int idx = 0;
          for (BatchData bd : cdata.compBatch) {
            bd.resetBatchData();
            while (bd.hasCurrent()) {
              if (cdata.bs.get(idx)) {
                idx++;
                continue;
              }
              vector.set(idx + soi, bd.getBoolean() ? 1 : 0);
              bd.next();
              idx++;
              ptsCount++;
            }
          }
          if (idx != rln) {
            System.out.println("Some math wrong");
          }
          break;
        }
        case INT64: {
          BigIntVector vector = bigIntVectors.get(cdata.name);
          reAllocTo(vector, vti);
          int idx = 0;
          for (BatchData bd : cdata.compBatch) {
            bd.resetBatchData();
            while (bd.hasCurrent()) {
              if (cdata.bs.get(idx)) {
                idx++;
                continue;
              }
              vector.set(idx + soi, bd.getLong());
              bd.next();
              idx++;
              ptsCount++;
            }
          }
          if (idx != rln) {
            System.out.println("Some math wrong");
          }
          break;
        }
        case VECTOR:
        case TEXT:
        case DOUBLE:
        case UNKNOWN:
        default:
          System.out.println(String.format("Unknown type chunk: %s.%s",
              processingDev,
              cdata.name));
          throw new Exception();
      }
    }
  }

  private void fillAlignedChunksIntoVectors() throws Exception{
    final int soi = this.locRowIdx;

    // set timestamps and calculate total length
    ChunkData timeChunk = null;
    for (ChunkData chunkData : collectedChunks) {
      if (chunkData.type == ChunkType.TIME) {
       timeChunk = chunkData;
       break;
     }
    }
    int offset = 0;
    for (long[] ta : timeChunk.timeBatch) {
      reAllocTo(timestampVector, offset + ta.length + soi);
      for (int i = 0; i < ta.length; i++) {
        timestampVector.set(soi + offset + i, ta[i]);
      }
      offset += ta.length;
    }

    final int vti = offset + soi;
    this.totalRowCount += offset;
    this.locRowIdx = vti;

    byte[] devByt = processingDev.getBytes(StandardCharsets.UTF_8);
    reAllocTo(timestampVector, vti);
    reAllocTo(idVector, vti);
    for (int i = 0; i < offset; i++) {
      idVector.set(soi + i, devByt);
    }

    ptsCount += 2 * offset;

    for (ChunkData cdata : collectedChunks) {
      if (cdata.type == ChunkType.TIME) {
        continue;
      }

      switch (vectorNameType.get(cdata.name)) {
        case FLOAT: {
          Float4Vector vector = float4Vectors.get(cdata.name);
          reAllocTo(vector, vti);
          int idx = 0, ofs = 0;
          for (TsPrimitiveType[] tpt : cdata.valueBatch) {
            while (idx < tpt.length) {
              vector.set(soi + ofs + idx, tpt[idx].getFloat());
              idx++;
            }
            ofs += tpt.length;
          }
          break;
        }
        case INT32: {
          IntVector vector = intVectors.get(cdata.name);
          reAllocTo(vector, vti);
          int idx = 0, ofs = 0;
          for (TsPrimitiveType[] tpt : cdata.valueBatch) {
            while (idx < tpt.length) {
              vector.set(soi + ofs + idx, tpt[idx].getInt());
              idx++;
            }
            ofs += tpt.length;
          }
          break;
        }
        case BOOLEAN: {
          BitVector vector = bitVectors.get(cdata.name);
          reAllocTo(vector, vti);
          int idx = 0, ofs = 0;
          for (TsPrimitiveType[] tpt : cdata.valueBatch) {
            while (idx < tpt.length) {
              vector.set(soi + ofs + idx, tpt[idx].getBoolean() ? 1 : 0);
              idx++;
            }
            ofs += tpt.length;
          }
          break;
        }
        case INT64: {
          BigIntVector vector = bigIntVectors.get(cdata.name);
          reAllocTo(vector, vti);
          int idx = 0, ofs = 0;
          for (TsPrimitiveType[] tpt : cdata.valueBatch) {
            while (idx < tpt.length) {
              vector.set(soi + ofs + idx, tpt[idx].getLong());
              idx++;
            }
            ofs += tpt.length;
          }
          break;
        }
        case VECTOR:
        case TEXT:
        case DOUBLE:
        case UNKNOWN:
        default:
          System.out.println(String.format("Unknown type chunk: %s.%s",
              processingDev,
              cdata.name));
          throw new Exception();
      }
    }
  }

  private void initOutput() throws Exception {
    fos = new FileOutputStream(dstPath);
    channel = fos.getChannel();

    List<FieldVector> vectors = new ArrayList<>();
    vectors.add(timestampVector);
    vectors.addAll(bigIntVectors.values());
    vectors.addAll(intVectors.values());
    vectors.addAll(float4Vectors.values());
    vectors.addAll(bitVectors.values());

    if (!largeVarCharVectors.isEmpty() || !float8Vectors.isEmpty()) {
      throw new Exception("Some vectors should not exist.");
    }

    // construct dictionary and provider
    DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
    LargeVarCharVector dictVector = new LargeVarCharVector(Field.nullable("dict", Types.MinorType.LARGEVARCHAR.getType()), allocator);
    reAllocTo(dictVector, devSets.size());
    int cnt = 0;
    for (String s : devSets) {
      dictVector.setSafe(cnt, s.getBytes(StandardCharsets.UTF_8));
      cnt++;
    }
    dictVector.setValueCount(devSets.size());
    idDict = new Dictionary(dictVector, new DictionaryEncoding(1L, false, null));
    provider.put(idDict);

    idVectorEncoded = (IntVector) DictionaryEncoder.encode(idVector, idDict);
    vectors.add(idVectorEncoded);

    root = new VectorSchemaRoot(vectors);
    writer = new ArrowFileWriter(root, provider, channel);
    writer.start();
  }

  private void writeCurrentBatch() throws IOException {
    root.setRowCount(locRowIdx);
    idVector.setValueCount(locRowIdx);
    timestampVector.setValueCount(locRowIdx);
    root.getFieldVectors().forEach(v -> v.setValueCount(locRowIdx));

    // encode deviceID
    IntVector v = (IntVector) DictionaryEncoder.encode(idVector, idDict);
    reAllocTo(idVectorEncoded, locRowIdx);
    for (int j = 0; j < v.getValueCount() && j < locRowIdx; j++) {
      idVectorEncoded.set(j, v.get(j));
    }

    writer.writeBatch();
    root.getFieldVectors().forEach(ValueVector::reset);
    idVector.reset();
    timestampVector.reset();
    idVectorEncoded.reset();
    locRowIdx = 0;
  }

  // endregion

  // region Reader and Verifier

  private void printMeta() throws Exception{
    try (FileInputStream fileInputStream = new FileInputStream(dstPath);
         ArrowFileReader reader = new ArrowFileReader(fileInputStream.getChannel(), allocator)) {
      // reader.loadNextBatch();
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

  // endregion

  /* to check meta */
  public static void main12(String[] args) throws Exception{
    TsFileSequentialConvertor convertor = new TsFileSequentialConvertor();
    convertor.printMeta();
    DevSenSupport dss = DevSenSupport.deserialize(convertor.supPath);
    System.out.println("ob");
  }

  /* to convert */
  public static void main(String[] args) throws Exception {
    long[] a = new long[] {1,3,4,6,8};
    long[] b = new long[] {2,5,7,9,10};
    long[] c = new long[] {11,13,14,16,18};
    long[] d = new long[] {1,23,34,46,58};

    long maxMemory = Runtime.getRuntime().maxMemory();
    long allocatedMemory = Runtime.getRuntime().totalMemory();
    long freeMemory = Runtime.getRuntime().freeMemory();

    System.out.println("Max Memory: " + maxMemory / 1024 / 1024 + " MB");
    System.out.println("Allocated Memory: " + allocatedMemory / 1024 / 1024 + " MB");
    System.out.println("Free Memory: " + freeMemory / 1024 / 1024 + " MB");

    try (BufferAllocator all = new RootAllocator(32 * 1024 * 1024 * 1024L)) {
      TsFileSequentialConvertor convertor = new TsFileSequentialConvertor(all);
      convertor.preprocess();
      convertor.reportChunkTypes();
      convertor.initOutput();
      convertor.processFile();
      convertor.closeWithoutAllocator();
    }

    System.out.println("OK");
  }

  public static void main9(String[] args) {
    TsFileSequentialConvertor convertor = new TsFileSequentialConvertor();
    // convertor.timestampVector.setValueCount(100);
    // convertor.idVector.setValueCount(100);
    // convertor.idVector.reAlloc();
    convertor.timestampVector.allocateNew(60);
    convertor.idVector.allocateNew(60);
    for (long i = 0; i < 100; i++) {
      if (i % 10 == 0) continue;
      convertor.timestampVector.set((int) i ,i);
      convertor.idVector.set((int)i, "A".getBytes(StandardCharsets.UTF_8));
    }

    for (long i = 0; i < 100; i++) {
      if (convertor.timestampVector.isNull((int)i)) continue;
      System.out.println(convertor.timestampVector.get((int)i));
      System.out.println(new String(convertor.idVector.get((int)i)));
    }
  }

}
