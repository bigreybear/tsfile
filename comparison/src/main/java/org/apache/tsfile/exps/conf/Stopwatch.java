package org.apache.tsfile.exps.conf;

public class Stopwatch {
  long acc, temp;
  public final void zero() {
    acc  = temp = 0;
  }

  public final void start() {
    temp = System.nanoTime();
  }

  public final void stop() {
    acc += System.nanoTime() - temp;
  }

  public final long report() {
    return acc;
  }

  public final long reportMilSecs() {
    return acc/ 1000000;
  }
}
