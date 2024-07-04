package org.apache.tsfile.exps.utils;

public class Stopwatch {
  String id;
  long acc, temp;

  public Stopwatch() {

  }

  public Stopwatch(String id) {
    this.id = id;
  }

  public final void clear() {
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

  public final void printResult() {
    if (id == null) {
      return;
    }

    System.out.println(String.format("%s: %d", this.id, acc));
  }

  public final long reportMilSecs() {
    return acc/ 1000000;
  }
}
