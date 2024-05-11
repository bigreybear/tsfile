package org.apache.tsfile.bmtool;
import java.io.IOException;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.parquet.bmtool.BMWriter;

public class Observer {
  public static long timeRecord = 0;
  public static void reportTimeLaps(String mark) {
    if (mark == null) {
      timeRecord = System.nanoTime();
      return;
    }
    long laps = System.nanoTime() - timeRecord;
    System.out.println(String.format("%s for %d nano seconds", mark, laps));
  }

  public static void main(String[] args) throws IOException, WriteProcessException {
    BMWriter.main(args);
  }
}
