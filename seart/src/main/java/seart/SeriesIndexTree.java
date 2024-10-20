package seart;

public interface SeriesIndexTree {
  void insert(String key, long value);

  long search(String sk);
}
