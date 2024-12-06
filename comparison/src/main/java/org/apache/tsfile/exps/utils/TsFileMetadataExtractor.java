package org.apache.tsfile.exps.utils;

import org.apache.tsfile.read.TsFileSequenceReader;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TsFileMetadataExtractor {

  public static void extract(String tsfile, String txt) {
    try (TsFileSequenceReader reader = new TsFileSequenceReader(tsfile)) {

      Map<String, List<String>> a = reader.getDeviceMeasurementsMap();
      List<String> path = new ArrayList<>();
      for (Map.Entry<String, List<String>> entry : a.entrySet()) {
        for (String s : entry.getValue()) {
          path.add(entry.getKey() + "." + s);
        }
      }

      Path filePath = Paths.get(txt);

      try (BufferedWriter writer = Files.newBufferedWriter(filePath)) {
        for (String str : path) {
          writer.write(str);
          writer.newLine();
        }
        System.out.println("Success to write " + txt);
      } catch (IOException e) {
        e.printStackTrace();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

    public static void main(String[] args) {
    extract("E:\\ExpDataSets\\Source-TsFile\\ZY.tsfile", "E:\\ExpDataSets\\Source-TsFile\\ZY.series");
  }
}
