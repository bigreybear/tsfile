package seart.utils;

public class Pair<L, R> {
  L left;
  R right;

  public Pair(L l, R r) {
    left = l;
    right = r;
  }

  public L left() {
    return left;
  }

  public R right() {
    return right;
  }

  @Override
  public String toString() {
    return String.format("<%s,%s>", left, right);
  }
}
