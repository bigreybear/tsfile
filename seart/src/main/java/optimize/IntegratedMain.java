package optimize;

public class IntegratedMain {

  public static void testVar(MyDataSet ds, AliasedArgs aliasedArgs, boolean estSpace, boolean log) {
    Main main = new Main();
    String param = "-alias" + aliasedArgs.name() + " -ds " + ds.name() + " -space -inspect";
    if (estSpace) main.mainbody(param.split(" "));

    param = "-alias " + aliasedArgs.name() + " -ds " + ds.name() + " -latency";
    for (int i = 0; i < 3; i++)
      main.mainbody(param.split(" "));
  }

  public static void main(String[] args) {
    boolean estSpace = false, log = true;

    AliasedArgs[] structures = new AliasedArgs[] {
        // AliasedArgs.MTree,
        AliasedArgs.ART,
        AliasedArgs.OLD_CDM,
        AliasedArgs.NEW_CDM
    };
    MyDataSet[] dataSets = new MyDataSet[] {
        MyDataSet.BW,
        MyDataSet.SW,
        MyDataSet.XYZC,
        MyDataSet.ZY
    };

    for (MyDataSet ds : dataSets){
      for (AliasedArgs alias : structures){
        testVar(ds, alias, estSpace, log);
      }
    }
  }
}
