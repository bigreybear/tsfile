package optimize;

import optimize.annotation.DebugOnly;

import java.util.Scanner;

// following VM options enables measurement
// --add-opens java.base/java.util=ALL-UNNAMED
public class IntegratedMain {

  private static void testVar(MyDataSet ds, AliasedArgs aliasedArgs, boolean estSpace, boolean log, int queryLoop) {
    Main main = new Main();
    String logFlag = log ? " -logPrint " : "";
    String param = "-alias " + aliasedArgs.name() + " -ds " + ds.name() + " -space -inspect" + logFlag;
    if (estSpace) main.mainbody(param.split(" "));

    param = "-alias " + aliasedArgs.name() + " -ds " + ds.name() + " -latency" + logFlag;
    for (int i = 0; i < queryLoop; i++) {
      main.mainbody(param.split(" "));
    }
  }

  // control inspection blocks that hurt performances.
  @DebugOnly
  public static final boolean INTERNAL_PROFILE = false;
  public static final float MIX_ALPHA = 0.5f;

  public static void main(String[] args) {
    boolean estSpace = true, logConsole = true;
    int queryLoop = 5;

    AliasedArgs[] structures = new AliasedArgs[] {
        AliasedArgs.MTree,
        AliasedArgs.ART,
        // AliasedArgs.OLD_CDM,
        // AliasedArgs.NEW_CDM,
        // AliasedArgs.HASH_SPC,
        AliasedArgs.HASH_MIX,
        AliasedArgs.HASH_TIM,
        // AliasedArgs.SORT_SPC,
        // AliasedArgs.SORT_MIX,
        AliasedArgs.SORT_TIM
    };
    MyDataSet[] dataSets = new MyDataSet[] {
        MyDataSet.BW,
        MyDataSet.SW,
        MyDataSet.XYZC,
        MyDataSet.ZY
    };

    checkPerfornaceUnimpacted();  // for internal options impacting performance
    for (MyDataSet ds : dataSets){
      for (AliasedArgs alias : structures){
        testVar(ds, alias, estSpace, logConsole, queryLoop);
      }
    }
  }

  public static void checkPerfornaceUnimpacted() {
    if (INTERNAL_PROFILE) {
      Scanner scanner = new Scanner(System.in);
      System.out.println("Some options impacting performance is enabled, making performance profile inaccurate, SURE to continue?");
      System.out.println("Enter Y to continue:");
      String input = scanner.nextLine();
      while (!input.equals("Y")) {
        System.out.println("Enter Y to continue:");
        input = scanner.nextLine();
      }
    }
  }
}
