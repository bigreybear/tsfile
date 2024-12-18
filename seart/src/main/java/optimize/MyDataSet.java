package optimize;

public enum MyDataSet {
  BW("text_series.txt", "baowu_query.txt"),
  SW("sw/path.txt", "sw/query.txt"),
  ZY("ZY.txt", "ZY-query.txt"),
  XYZC("xyzc/boxmeas_path.txt", "xyzc/boxmeas_query.txt");

  public final String rfile;
  public final String qfile;

  MyDataSet(String a, String b) {
    rfile = "mtreedata/" + a;
    qfile = "mtreedata/" + b;
  }
}
