package org.apache.tsfile.exps.utils;

import org.apache.commons.collections.map.HashedMap;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.header.ChunkGroupHeader;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.reader.page.PageReader;
import org.apache.tsfile.read.reader.page.TimePageReader;
import org.apache.tsfile.read.reader.page.ValuePageReader;
import org.apache.tsfile.utils.TsPrimitiveType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class TsFileAnalyst {

  // private static final String tarFile = "F:\\0006DataSets\\CCS.tsfile";
  public String tarFile = "E:\\ExpDataSets\\Source-TsFile\\CCS.tsfile";

  public static void main(String[] args) throws IOException {
    TsFileAnalyst analyst = new TsFileAnalyst();
    TsFileAnalyst a2 = new TsFileAnalyst();
    analyst.tarFile = "F:\\0006DataSets\\CCS.tsfile";
    a2.tarFile = "F:\\0006DataSets\\Results\\ZY_UNCOMPRESSED.tsfile";
    // analyst.analyze();
    a2.analyze();
    System.out.println("Analysis finished.");
  }

  public void analyze() throws IOException {
    try (TsFileSequenceReader reader = new TsFileSequenceReader(tarFile)) {
      fileSize = reader.fileSize();
      dataSize = fileSize - reader.getAllMetadataSize();

      float lastProgressReported = 0.0f, progress = 0.0f;
      // Note(zx) start of the file
      reader.position((long) TSFileConfig.MAGIC_STRING.getBytes().length + 1);
      List<long[]> timeBatch = new ArrayList<>();
      int pageIndex = 0;
      byte marker;
      while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {

        progress = reader.position() * 1.0f / dataSize;
        if (progress - lastProgressReported > 0.1) {
          lastProgressReported = progress;
          String time = DateTimeFormatter.ofPattern("HH:mm:ss").format(java.time.LocalDateTime.now());
          System.out.println(String.format(
              "Analyzing progress: %f, at time: %s",
              progress, time));
        }

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
              // throw new RuntimeException("Unsupported to VECTOR type.");
            }

            BaseChunk bc = null;
            long pos = reader.position();
            while (dataSize > 0) {
              valueDecoder.reset();
              PageHeader pageHeader =
                  reader.readPageHeader(
                      header.getDataType(),
                      (header.getChunkType() & 0x3F) == MetaMarker.CHUNK_HEADER);

              // readPage has advanced the reader pointer
              ByteBuffer pageData = reader.readPage(pageHeader, header.getCompressionType());

              if (pageIndex != 0) {
                // must have initialized BaseChunk
                bc.updateSize(pageHeader.getCompressedSize(), pageHeader.getUncompressedSize());
              }

              // Time Chunk
              if ((header.getChunkType() & TsFileConstant.TIME_COLUMN_MASK)
                  == TsFileConstant.TIME_COLUMN_MASK) {
                TimePageReader timePageReader =
                    new TimePageReader(pageHeader, pageData, defaultTimeDecoder);
                timeBatch.add(timePageReader.getNextTimeBatch());
                if (pageIndex == 0) {
                  bc = new TimeChunk(pos, pageHeader.getCompressedSize(), pageHeader.getUncompressedSize());
                }
                // Value Chunk
              } else if ((header.getChunkType() & TsFileConstant.VALUE_COLUMN_MASK)
                  == TsFileConstant.VALUE_COLUMN_MASK) {
                if (pageIndex == 0) {
                  appendSensorMapping(header.getMeasurementID());
                  bc = new ChunkRep(pos, header.getDataType(), header.getEncodingType(), header.getCompressionType());
                }
                ValuePageReader valuePageReader =
                    new ValuePageReader(pageHeader, pageData, header.getDataType(), valueDecoder);
                TsPrimitiveType[] valueBatch =
                    valuePageReader.nextValueBatch(timeBatch.get(pageIndex));
                bc.pts += valueBatch.length;
                // NonAligned Chunk
              } else {
                if (pageIndex == 0) {
                  appendSensorMapping(header.getMeasurementID());
                  bc = new ChunkRep(pos, header.getDataType(), header.getEncodingType(), header.getCompressionType());
                }
                PageReader pageReader =
                    new PageReader(
                        pageData, header.getDataType(), valueDecoder, defaultTimeDecoder);
                BatchData batchData = pageReader.getAllSatisfiedPageData();
                bc.pts += batchData.count;
              }
              pageIndex++;
              dataSize -= pageHeader.getSerializedPageSize();
            }

            if (bc.isTimeChunk()) {
              deviceReps.get(curDev).timeChunks.add((TimeChunk) bc);
            } else {
              deviceReps.get(curDev).appendChunk(header.getMeasurementID(), (ChunkRep) bc);
            }

            // summary one chunk which has finished
            break;
          case MetaMarker.CHUNK_GROUP_HEADER:
            curDev = appendChunkGroupHeader(reader.readChunkGroupHeader(), reader.position());
            if (curDev.equals("dacoo.BF116_BFHydSta_BFHydSta_BleedSolVlv_South")) {
              System.out.println("DEBUG");
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

  private final StringBuilder reporter = new StringBuilder();
  private final Map<String, DeviceRep> deviceReps = new TreeMap<>();
  private final List<DeviceRep> orderedDevices = new ArrayList<>();
  private final List<String> orderedDeviceNames = new ArrayList<>();
  private final Map<String, Set<String>> sensorMapping = new HashedMap();
  private long fileSize, dataSize;
  private String curDev;

  private void appendSensorMapping(String sen) {
    if (!sensorMapping.containsKey(sen)) {
      sensorMapping.put(sen, new HashSet<>());
    }
    sensorMapping.get(sen).add(curDev);
  }

  private String appendChunkGroupHeader(ChunkGroupHeader header, long pos) {
    String dev = header.getDeviceID();
    DeviceRep rep = deviceReps.get(dev);
    if (rep == null) {
      deviceReps.put(dev, new DeviceRep());
      rep = deviceReps.get(dev);
    }
    orderedDevices.add(rep);
    orderedDeviceNames.add(header.getDeviceID());

    rep.ckgNum ++;
    rep.ckgPos.add(pos);
    return dev;
  }

  private class DeviceRep {
    int ckgNum = 0;
    Map<String, SensorRep> sensorReps = new HashMap<>();
    List<Long> ckgPos = new ArrayList<>();
    List<TimeChunk> timeChunks = new ArrayList<>();

    private void appendChunk(String sen, ChunkRep cr) {
      SensorRep rep = sensorReps.get(sen);
      if (rep == null) {
        rep = new SensorRep();
        sensorReps.put(sen, rep);
      }

      rep.chunkReps.add(cr);
      rep.ttlPts += cr.pts;
      rep.compSize += cr.compSize;
      rep.uncompSize += cr.uncompSize;
    }

    void showChunkGroupDistribution() {
      StringBuilder builder = new StringBuilder();
      for (Long l : ckgPos) {
        builder.append(" ").append(1.0f * l / fileSize);
      }
      System.out.println(builder);
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      for (Map.Entry<String, SensorRep> e : sensorReps.entrySet()) {
        builder.append("{");
        builder.append(e.getKey()).append(",");
        builder.append(e.getValue().ttlPts / 1000).append("k").append(",");
        builder.append(String.format("%.2f", e.getValue().compSize * 1.0f/ e.getValue().uncompSize)).append("} ");
      }
      return builder.toString();
    }
  }

  private class SensorRep {
    int ttlPts = 0, compSize = 0, uncompSize = 0;
    List<ChunkRep> chunkReps = new ArrayList<>();
  }

  private abstract class BaseChunk {
    long pos;
    int compSize, uncompSize, pts = 0;

    void updateSize(int cs, int ucs) {
      compSize += cs;
      uncompSize += ucs;
    }

    boolean isTimeChunk() {
      return false;
    }
  }

  private class TimeChunk extends BaseChunk{
    public TimeChunk(long pos, int compSize, int uncompSize) {
      this.pos = pos;
      this.compSize = compSize;
      this.uncompSize = uncompSize;
    }

    boolean isTimeChunk() {
      return true;
    }
  }

  private class ChunkRep extends BaseChunk{
    TSDataType type;
    TSEncoding encoding;
    CompressionType compressionType;

    public ChunkRep(long pos, TSDataType type, TSEncoding encoding, CompressionType compressionType) {
      this.pos = pos;
      this.type = type;
      this.encoding = encoding;
      this.compressionType = compressionType;
    }
  }
}
