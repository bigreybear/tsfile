package org.apache.tsfile.exps;

import org.apache.tsfile.file.header.ChunkGroupHeader;
import org.apache.tsfile.read.TsFileDeviceIterator;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;

import java.io.IOException;
import java.util.List;

public class ZYLoader {

  public void processTsFile() throws IOException {
    String path = "E:\\0006DataSets\\ZY.tsfile";
    TsFileSequenceReader sequenceReader = new TsFileSequenceReader(path);
    TsFileReader reader = new TsFileReader(sequenceReader);
    List<Path> paths =  sequenceReader.getAllPaths();
    System.out.println("hello");
    ChunkGroupHeader header;
    header = sequenceReader.readChunkGroupHeader();
    TsFileDeviceIterator iterator = sequenceReader.getAllDevicesIteratorWithIsAligned();


  }

  public static void main(String[] args) throws IOException {
    ZYLoader loader = new ZYLoader();
    loader.processTsFile();
  }
}
