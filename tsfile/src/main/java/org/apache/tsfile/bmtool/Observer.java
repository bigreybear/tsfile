package org.apache.tsfile.bmtool;

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
}
