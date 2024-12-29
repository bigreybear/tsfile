package optimize;

import static optimize.merge.SuffixMergeVDev.collectSuffixes;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import optimize.merge.MapType;
import optimize.merge.MergePrefixVDev;
import optimize.merge.PrefixMergeStrategy;
import optimize.nodes.NodeInspector;
import org.openjdk.jol.info.GraphLayout;

public class Main {

  // configurations
  public static final boolean CDM_WITH_EF = false;

  public static final StringBuilder REPORT_CHANNEL = new StringBuilder();

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

    res += " -merge";
    res += " -ms full";
    // res += " -ms partial";
    // res += " -ms simple";

    // res += " -ds bw";
    // res += " -ds xyzc";
    // res += " -ds sw";
    res += " -ds zy";

    res += " -twoLevel";
    res += " -latency";
    res += " -space";
    res += " -spaceDetail";
    res += " -depth";
    // res += " -template";
    res += " -inspect";

    return res.split(" ");
  }

  public static MyDataSet dataSet;
  public static PrefixMergeStrategy mergeStrategy;
  public static MapType mapType;

  public static void main(String[] args) {
    ExpResultLogger resultPrinter = new ExpResultLogger();
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

    TSTree tree = MainSupport.buildLogicalTree(dataSet, argList.contains("-twoLevel"));
    resultPrinter.twoLevel = argList.contains("-twoLevel");

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
                "After template space: %d \n", _space = GraphLayout.parseInstance(tree).totalSize()));
      }
    }

    if (argList.contains("-space")) {
      long r = MainSupport.measureSpace(tree, mergeStrategy, mapType, argList.contains("-spaceDetail"));
      resultPrinter.space = r;
      resultPrinter.recordSpace();
    }

    if (argList.contains("-latency")) {
      long l = MainSupport.estimateLatency(tree, dataSet, mergeStrategy, mapType);
      resultPrinter.latency = l;
      resultPrinter.recordLatency();
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
}
