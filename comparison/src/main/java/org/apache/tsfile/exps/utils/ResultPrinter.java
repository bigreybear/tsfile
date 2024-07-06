package org.apache.tsfile.exps.utils;

import org.apache.tsfile.exps.conf.FileScheme;
import org.apache.tsfile.exps.conf.MergedDataSets;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class ResultPrinter {
  BufferedWriter writer;
  FileScheme scheme;
  MergedDataSets dataSets;
  public StringBuilder builder = new StringBuilder();

  public ResultPrinter(String filePath, boolean append) throws IOException {
    writer = new BufferedWriter(new FileWriter(filePath, append));
  }

  public void close() throws IOException {
    writer.close();
  }

  public void setStatus(FileScheme scheme, MergedDataSets dataSets) {
    this.scheme = scheme;
    this.dataSets = dataSets;
  }

  public void writeResult(float[] res)
      throws IOException {
    // file_scheme data_set data_size index_size data_time index_time, all time are mil-seconds
    String c = String.format("%s\t\t%s\t%.0f\t%.0f\t%.0f\t%.0f\t%s\n",
        scheme.name(), dataSets.name(),
        res[0], res[1],
        res[2], res[3],
        LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
    builder.append(c);
    writer.write(c);
  }

  public void log(String content) throws IOException {
    String c =String.format("%s\t%s\t%s\t%s\n",
        scheme.name(), dataSets.name(), content,
        LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
    builder.append(c);
    writer.write(c);
  }
}
