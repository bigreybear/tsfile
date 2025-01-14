package optimize;

public class IntegratedMain {

  public static void allDataSetOnMTree() {
    Main.dataAlias = "MTree";
    for (MyDataSet mds : MyDataSet.values()) {
      Main main = new Main();
      String param = "-mt hash -ds " + mds.name() + " -space -inspect";
      main.mainbody(param.split(" "));

      param = "-mt hash -ds " + mds.name() + " -latency";
      for (int i = 0; i < 3; i++)
        main.mainbody(param.split(" "));
    }
  }

  public static void testART() {
    Main.dataAlias = "ART";
    for (MyDataSet mds : MyDataSet.values()) {
      Main main = new Main();
      String param = "-mt fdm -merge -ds " + mds.name() + " -ms full -oneTree -space -inspect";
      main.mainbody(param.split(" "));

      param = "-mt fdm -merge -ds " + mds.name() + " -ms full -oneTree -latency";
      for (int i = 0; i < 3; i++)
        main.mainbody(param.split(" "));
    }
  }

  public static void testCDM4() {
    Main.dataAlias = "CDM4";
    for (MyDataSet mds : MyDataSet.values()) {
      Main main = new Main();
      String param = "-mt cdm -merge -ds " + mds.name() + " -ms full -oneTree -space -inspect";
      main.mainbody(param.split(" "));

      param = "-mt cdm -merge -ds " + mds.name() + " -ms full -oneTree -latency";
      for (int i = 0; i < 3; i++)
        main.mainbody(param.split(" "));
    }
  }

  public static void testNewCDM() {
    Main.dataAlias = "NewCDM";
    for (MyDataSet mds : MyDataSet.values()) {
      Main main = new Main();
      String param = "-mt ncdm -merge -ds " + mds.name() + " -ms full -oneTree -space -inspect";
      // main.mainbody(param.split(" "));

      param = "-mt ncdm -merge -ds " + mds.name() + " -ms full -oneTree -latency";
      for (int i = 0; i < 3; i++)
        main.mainbody(param.split(" "));
    }
  }

  public static void main(String[] args) {
    // allDataSetOnMTree();
    // testART();
    // testCDM4();
    testNewCDM();
  }
}
