package org.apache.tsfile.exps.utils;

import org.apache.tsfile.exps.conf.FileScheme;
import org.apache.tsfile.exps.conf.MergedDataSets;
import org.apache.tsfile.exps.updated.BenchReader;
import org.apache.tsfile.exps.updated.BenchWriter;

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

  /**
   * param: [data_size, index_size, data_time(ms), index_time]
   */
  public void writeResult(long[] res)
      throws IOException {
    setStatus(BenchWriter.currentScheme, BenchWriter.mergedDataSets);

    // file_scheme data_set data_size index_size data_time index_time, all time are mil-seconds
    String c = String.format("%s\t%s\t%d\t%d\t%d\t%d\t%s\n",
        scheme.name(), dataSets.name(),
        res[0], res[1],
        res[2], res[3],
        LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
    builder.append(c);
    writer.write(c);
  }

  public void queryResult(long[] res, String filePath) throws IOException {
    // total time, query num, rec num
    String c = String.format("%s\t%s\t%s\t%d\t%d\t%d\t%d\t%s\t%s\n",
        dataSets.name(),
        BenchReader.QTYPE.name(),
        scheme.name(),
        res[0], res[1], res[2],
        res[0] / res[1], filePath,
        LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
    builder.append(c);
    writer.write(c);
    System.out.print(c);
  }

  public void log(String content) throws IOException {
    String c =String.format("%s\t%s\t%s\t%s\n",
        scheme.name(), dataSets.name(), content,
        LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
    builder.append(c);
    writer.write(c);
  }
}
