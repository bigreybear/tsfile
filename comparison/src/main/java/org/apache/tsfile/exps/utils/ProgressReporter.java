package org.apache.tsfile.exps.utils;

import java.time.format.DateTimeFormatter;

public class ProgressReporter {
  private long totalPts;
  private float repInterval;
  private long lastPts;
  private float lastReportedPrg;
  private String id = "default";

  public ProgressReporter(long totalPoints, float reportInterval) {
    totalPts = totalPoints;
    repInterval = reportInterval;
    lastPts = 0;
    lastReportedPrg = 0;
  }

  public ProgressReporter(long ttlPts, String id) {
    this(ttlPts);
    this.id = id;
  }

  public ProgressReporter(long ttlPts) {
    this(ttlPts, 0.1f);
  }

  public ProgressReporter() {
    this(100);
  }

  public ProgressReporter(String id) {
    this();
    setID(id);
  }

  public void setID(String id) {
    this.id = id;
  }

  public void setTotalPoints(long totalPoints) {
    totalPts = totalPoints;
  }

  public void setInterval(float f) {
    repInterval = f;
  }

  public void report(long currentPoints) {
    float curPgr =  currentPoints * 1.0f / totalPts;
    if (curPgr - lastReportedPrg < repInterval) {
      return;
    }
    lastReportedPrg = curPgr;
    printReport();
  }

  public void addProgressAndReport(long add) {
    report(add + lastPts);
    lastPts += add;
  }

  private void printReport() {
    String time = DateTimeFormatter.ofPattern("HH:mm:ss").format(java.time.LocalDateTime.now());
    System.out.println(String.format("Progress [%s] at %.2f, time: %s",
        id, lastReportedPrg, time));
  }
}
