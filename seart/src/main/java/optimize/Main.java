package optimize;

import static optimize.merge.SuffixMergeVDev.collectSuffixes;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;
import optimize.merge.MapType;
import optimize.merge.MergePrefixVDev;
import optimize.merge.PrefixMergeStrategy;
import optimize.nodes.NodeInspector;
import org.openjdk.jol.info.GraphLayout;

public class Main {

  public static final Field tableField;

  static {
    try {
      tableField = HashMap.class.getDeclaredField("table");
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
  }

  public static String[] defaultArgs() {
    String res = "";
    // res += " -mt hash";
    // res += " -mt fdm";
    res += " -mt cdm";
    // res += " -mt ncdm";

    res += " -merge";
    res += " -ms full";
    // res += " -ms partial";
    // res += " -ms simple";

    // res += " -ds bw";
    // res += " -ds sw";
    res += " -ds xyzc";
    // res += " -ds zy";

    res += " -oneTree";
    res += " -latency";
    res += " -space";
    res += " -spaceDetail";
    // res += " -depth";
    // res += " -template";
    res += " -inspect";

    // res += " -profile";

    return res.split(" ");
  }

  // global args
  public static final boolean CDM_WITH_EF = false;
  public static final StringBuilder REPORT_CHANNEL = new StringBuilder();
  public static String dataAlias = "NoN";

  // local args
  public MyDataSet dataSet;
  public PrefixMergeStrategy mergeStrategy;
  public MapType mapType;

  public void mainbody(String[] args) {
    resetStaticArgs();
    ExpResultLogger resultPrinter = new ExpResultLogger(dataAlias);
    System.out.println(MainSupport.getBuildTimestamp());
    args = args.length == 0 ? defaultArgs() : args;
    List<String> argList = Arrays.stream(args).distinct().collect(Collectors.toList());
    if (argList.size() != args.length) throw new RuntimeException("duplicated args.");
    int argIdx = 0;

    if ((argIdx = argList.indexOf("-ms")) != -1) {
      mergeStrategy = PrefixMergeStrategy.valueOf(argList.get(argIdx + 1).toUpperCase());
      resultPrinter.pms = mergeStrategy;
    }

    if ((argIdx = argList.indexOf("-ds")) != -1) {
      dataSet = MyDataSet.valueOf(argList.get(argIdx + 1).toUpperCase());
      resultPrinter.mds = dataSet;
    }

    if ((argIdx = argList.indexOf("-mt")) != -1) {
      mapType = MapType.valueOf(argList.get(argIdx + 1).toUpperCase());
      resultPrinter.mapType = mapType;
      if (mapType.equals(MapType.FDM)) {
        mergeStrategy = PrefixMergeStrategy.FULL;
        resultPrinter.pms = mergeStrategy;
      }

      if (mapType.equals(MapType.HASH)) {
        tableField.setAccessible(true);
      }
    }

    TSTree tree = MainSupport.buildLogicalTree(dataSet, argList.contains("-oneTree"));
    resultPrinter.oneTree = argList.contains("-oneTree");

    if (argList.contains("-merge")) {
      MergePrefixVDev.mergePrefixes(tree, mapType, mergeStrategy);
    } else {
      mergeStrategy = PrefixMergeStrategy.NO_MERGE;
      resultPrinter.pms = mergeStrategy;
    }

    long _space = -1L;
    if (argList.contains("-template")) {
      if (argList.contains("-space")) {
        collectSuffixes(tree, mapType, false);
        REPORT_CHANNEL.append(
            String.format(
                "Before template space: %d \n", GraphLayout.parseInstance(tree).totalSize()));
      }

      collectSuffixes(tree, mapType, true);

      if (argList.contains("-space")) {
        REPORT_CHANNEL.append(
            String.format(
                "After template space: %d \n",
                _space = GraphLayout.parseInstance(tree).totalSize()));
      }
    }

    if (argList.contains("-space")) {
      long r =
          MainSupport.measureSpace(tree, mergeStrategy, mapType, argList.contains("-spaceDetail"));
      resultPrinter.space = r;
      resultPrinter.recordSpace();
    }

    int loop = 1;
    if (argList.contains("-profile")) {
      Scanner scanner = new Scanner(System.in);
      System.out.println("Estimate latency for how many times:");
      String input = scanner.nextLine();
      loop = Integer.valueOf(input);
      while (loop != 0) {
        MainSupport.estimateLatency(tree, dataSet, mergeStrategy, mapType);

        if (loop-- == 1) {
          System.out.println("Last loop finished, again? enter -1 to equit.");
          loop = Integer.valueOf(scanner.nextLine());
        }

        if (loop == -1) break;
      }
    }

    if (argList.contains("-latency")) {
      for (int i = 0; i < loop; i++) {
        resultPrinter.latency = MainSupport.estimateLatency(tree, dataSet, mergeStrategy, mapType);
        resultPrinter.recordLatency();
      }
    }

    if (argList.contains("-inspect") || argList.contains("-depth")) {
      NodeInspector ni = new NodeInspector();
      ni.inspect(tree.root);
      if (argList.contains("-inspect")) {
        System.out.println(ni);
      }

      if (argList.contains("-depth")) {
        ni.dumpDepthResults(resultPrinter);
      }
    }

    REPORT_CHANNEL.append("FINISH:" + String.join(" ", argList) + " with EF code: " + CDM_WITH_EF);
    REPORT_CHANNEL.append("\n");
    System.out.println(REPORT_CHANNEL);
  }

  private void resetStaticArgs() {
    REPORT_CHANNEL.delete(0, REPORT_CHANNEL.length());
  }

  public static void main(String[] args) {
    Main m = new Main();
    m.mainbody(args);
  }
}
