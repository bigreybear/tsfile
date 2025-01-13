package optimize;

public class IntegratedMain {

  public static void allDataSetOnCDM() {
    for (MyDataSet mds : MyDataSet.values()) {
      Main main = new Main();
      String param = "-mt cdm -merge -ds " + mds.name() + " -ms full -oneTree -latency -inspect";
      main.mainbody(param.split(" "));
    }
  }

  public static void main(String[] args) {
    allDataSetOnCDM();
  }
}
