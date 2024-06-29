/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.hadoop;

import static java.lang.Math.max;
import static java.lang.Math.min;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.values.bloomfilter.BloomFilterWriteStore;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputCompressor;
import org.apache.parquet.crypto.InternalFileEncryptor;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.api.WriteSupport.FinalizedWriteContext;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * NOTE From UML, it holds many crucial class instance.
 * NOTE directly controlled/called by {@link ParquetWriter}
 * Note(zx): Change from default to public.
 * @param <T>
 */
public class InternalParquetRecordWriter<T> {
  private static final Logger LOG = LoggerFactory.getLogger(InternalParquetRecordWriter.class);

  private final ParquetFileWriter parquetFileWriter;
  private final WriteSupport<T> writeSupport;
  private final MessageType schema;
  private final Map<String, String> extraMetaData;
  private final long rowGroupSize;
  private long rowGroupSizeThreshold;
  private long nextRowGroupSize;
  private final BytesInputCompressor compressor;
  private final boolean validating;
  private final ParquetProperties props;

  private boolean closed;

  private long recordCount = 0;
  private long recordCountForNextMemCheck;
  private long lastRowGroupEndPos = 0;

  private ColumnWriteStore columnStore;
  private ColumnChunkPageWriteStore pageStore;
  private BloomFilterWriteStore bloomFilterWriteStore;
  private RecordConsumer recordConsumer;

  private InternalFileEncryptor fileEncryptor;
  private int rowGroupOrdinal;

  /**
   * @param parquetFileWriter the file to write to
   * @param writeSupport      the class to convert incoming records
   * @param schema            the schema of the records
   * @param extraMetaData     extra meta data to write in the footer of the file
   * @param rowGroupSize      the size of a block in the file (this will be approximate)
   * @param compressor        the codec used to compress
   */
  public InternalParquetRecordWriter(
      ParquetFileWriter parquetFileWriter,
      WriteSupport<T> writeSupport,
      MessageType schema,
      Map<String, String> extraMetaData,
      long rowGroupSize,
      BytesInputCompressor compressor,
      boolean validating,
      ParquetProperties props) {
    this.parquetFileWriter = parquetFileWriter;
    this.writeSupport = Objects.requireNonNull(writeSupport, "writeSupport cannot be null");
    this.schema = schema;
    this.extraMetaData = extraMetaData;
    this.rowGroupSize = rowGroupSize;
    this.rowGroupSizeThreshold = rowGroupSize;
    this.nextRowGroupSize = rowGroupSizeThreshold;
    this.compressor = compressor;
    this.validating = validating;
    this.props = props;
    this.fileEncryptor = parquetFileWriter.getEncryptor();
    this.rowGroupOrdinal = 0;
    initStore();
    recordCountForNextMemCheck = props.getMinRowCountForPageSizeCheck();
  }

  public ParquetMetadata getFooter() {
    return parquetFileWriter.getFooter();
  }

  private void initStore() {
    // NOTE Store holds ColumnChunkPageWriter(s) each for a field
    ColumnChunkPageWriteStore columnChunkPageWriteStore = new ColumnChunkPageWriteStore(
        compressor,
        schema,
        props.getAllocator(),
        props.getColumnIndexTruncateLength(),
        props.getPageWriteChecksumEnabled(),
        fileEncryptor,
        rowGroupOrdinal);
    // NOTE these 2 stores are just IDENTICAL
    pageStore = columnChunkPageWriteStore;
    bloomFilterWriteStore = columnChunkPageWriteStore;

    columnStore = props.newColumnWriteStore(schema, pageStore, bloomFilterWriteStore);
    // NOTE prepare column DL/RL, type definitions, name, index, parental hierarchy
    MessageColumnIO columnIO = new ColumnIOFactory(validating).getColumnIO(schema);
    this.recordConsumer = columnIO.getRecordWriter(columnStore);
    writeSupport.prepareForWrite(recordConsumer);
  }

  public void close() throws IOException, InterruptedException {
    if (!closed) {
      long lastFlushData = System.nanoTime();

      flushRowGroupToStore();
      FinalizedWriteContext finalWriteContext = writeSupport.finalizeWrite();
      Map<String, String> finalMetadata = new HashMap<String, String>(extraMetaData);
      String modelName = writeSupport.getName();
      if (modelName != null) {
        finalMetadata.put(ParquetWriter.OBJECT_MODEL_NAME_PROP, modelName);
      }
      finalMetadata.putAll(finalWriteContext.getExtraMetaData());

      dataSize = parquetFileWriter.out.getPos();

      parquetFileWriter.out.force();
      flushDataTime += System.nanoTime() - lastFlushData;

      parquetFileWriter.end(finalMetadata);
      flushIndexTime = parquetFileWriter.reportFlushIndextime();
      indexSize = parquetFileWriter.reportIndexEndPos() - dataSize + parquetFileWriter.getFootSize();

      closed = true;
    }
  }

  long flushDataTime, flushIndexTime, dataSize, indexSize;

  public void report(BufferedWriter bw) throws IOException {
    bw.write(String.format("DataFlushTime: %d, IndexFlushTIme: %d\n", flushDataTime/1000000, flushIndexTime/1000000));
    bw.write(String.format("DataSize: %d, IndexSize: %d\n", dataSize, indexSize));
  }

  public float[] report() {
    return new float[] {dataSize, indexSize, (float) flushDataTime /1000000, (float) flushIndexTime /1000000};
  }

  public void write(T value) throws IOException, InterruptedException {
    // Note(zx) directly called by ParquetWriter
    long flushData = System.nanoTime();
    writeSupport.write(value);
    ++recordCount;
    checkBlockSizeReached();
    flushDataTime += System.nanoTime() - flushData;
  }

  /**
   * @return the total size of data written to the file and buffered in memory
   */
  public long getDataSize() {
    return lastRowGroupEndPos + columnStore.getBufferedSize();
  }

  private void checkBlockSizeReached() throws IOException {
    // Note(zx) just isomorphic to size check as TsFile write process
    if (recordCount
        >= recordCountForNextMemCheck) { // checking the memory size is relatively expensive, so let's not do it
      // for every record.
      long memSize = columnStore.getBufferedSize();
      long recordSize = memSize / recordCount;
      // flush the row group if it is within ~2 records of the limit
      // it is much better to be slightly under size than to be over at all
      if (memSize > (nextRowGroupSize - 2 * recordSize)) {
        LOG.debug("mem size {} > {}: flushing {} records to disk.", memSize, nextRowGroupSize, recordCount);
        flushRowGroupToStore();
        initStore();
        recordCountForNextMemCheck = min(
            max(props.getMinRowCountForPageSizeCheck(), recordCount / 2),
            props.getMaxRowCountForPageSizeCheck());
        this.lastRowGroupEndPos = parquetFileWriter.getPos();
      } else {
        recordCountForNextMemCheck = min(
            max(
                props.getMinRowCountForPageSizeCheck(),
                (recordCount + (long) (nextRowGroupSize / ((float) recordSize)))
                    / 2), // will check halfway
            recordCount
                + props.getMaxRowCountForPageSizeCheck() // will not look more than max records ahead
            );
        LOG.debug("Checked mem at {} will check again at: {}", recordCount, recordCountForNextMemCheck);
      }
    }
  }

  private void flushRowGroupToStore() throws IOException {
    recordConsumer.flush();
    LOG.debug("Flushing mem columnStore to file. allocated memory: {}", columnStore.getAllocatedSize());
    if (columnStore.getAllocatedSize() > (3 * rowGroupSizeThreshold)) {
      LOG.warn("Too much memory used: {}", columnStore.memUsageString());
    }

    if (recordCount > 0) {
      rowGroupOrdinal++;
      parquetFileWriter.startBlock(recordCount);
      columnStore.flush();
      pageStore.flushToFileWriter(parquetFileWriter);
      recordCount = 0;
      parquetFileWriter.endBlock();
      this.nextRowGroupSize = Math.min(parquetFileWriter.getNextRowGroupSize(), rowGroupSizeThreshold);
    }

    columnStore.close();
    columnStore = null;
    pageStore = null;
  }

  long getRowGroupSizeThreshold() {
    return rowGroupSizeThreshold;
  }

  void setRowGroupSizeThreshold(long rowGroupSizeThreshold) {
    this.rowGroupSizeThreshold = rowGroupSizeThreshold;
  }

  MessageType getSchema() {
    return this.schema;
  }
}
