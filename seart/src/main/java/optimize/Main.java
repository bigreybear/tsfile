package optimize;

import static optimize.merge.SuffixMergeVDev.collectSuffixes;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.stream.Collectors;
import optimize.merge.MapType;
import optimize.merge.MergePrefixVDev;
import optimize.merge.PrefixMergeStrategy;
import optimize.nodes.NodeInspector;
import optimize.util.InternalInspector;
import optimize.util.LoggedPrintStream;
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

    // res += "-alias MTREE";
    // res += "-alias ART";
    // res += "-alias OLD_CDM";
    res += "-alias NEW_CDM";

    res += " -ds bw";
    // res += " -ds sw";
    // res += " -ds xyzc";
    // res += " -ds zy";

    // res += " -space";
    res += " -latency";
    res += " -inspect";

    // res += " -profile";
    // res += " -template";
    res += " -logPrint";

    return res.split(" ");
  }

  // global args
  public static final boolean CDM_WITH_EF = false;
  public static final StringBuilder REPORT_CHANNEL = new StringBuilder();
  public static final boolean OBSERVE_METRIC = true; // impact performance significantly
  public static String dataAlias = "NoN";

  // local args
  MyDataSet dataSet;
  boolean flatTree;
  PrefixMergeStrategy mergeStrategy;
  MapType mapType;
  boolean mergeSuffix, inspect, profile;
  boolean estSpace, estLatency, logPrint;

  public void mainbody(String[] args) {
    resetStaticArgs();
    ExpResultLogger resultPrinter = new ExpResultLogger(dataAlias);
    System.out.println(MainSupport.getBuildTimestamp());
    args = args.length == 0 ? defaultArgs() : args;
    List<String> argList = Arrays.stream(args).distinct().collect(Collectors.toList());
    if (argList.size() != args.length) throw new RuntimeException("duplicated args.");
    int argIdx = 0;


    setByArgs(args);
    if (logPrint) System.setOut(new LoggedPrintStream(System.out, "print_logs.txt"));
    resultPrinter.mds = dataSet;
    resultPrinter.pms = mergeStrategy;
    resultPrinter.mapType = mapType;
    resultPrinter.oneTree = flatTree;
    resultPrinter.alias = dataAlias;

    if (mapType.equals(MapType.HASH)) {
      tableField.setAccessible(true);
    }

    TSTree tree = MainSupport.buildLogicalTree(dataSet, flatTree);
    if (mergeStrategy != PrefixMergeStrategy.NO_MERGE) {
      MergePrefixVDev.mergePrefixes(tree, mapType, mergeStrategy);
    }

    // space estimation is coupled with suffix-merging
    if (estSpace) {
      long _space = -1L;
      if (mergeSuffix) {
        collectSuffixes(tree, mapType, false);
        REPORT_CHANNEL.append(
            String.format(
                "Before template space: %d \n", GraphLayout.parseInstance(tree).totalSize()));
        collectSuffixes(tree, mapType, true);
        REPORT_CHANNEL.append(
            String.format(
                "After template space: %d \n",
                _space = GraphLayout.parseInstance(tree).totalSize()));
      } else {
        _space =
            MainSupport.measureSpace(tree, mergeStrategy, mapType, inspect);
      }
      resultPrinter.space = _space;
      resultPrinter.recordSpace();
    } else {
      if (mergeSuffix) {
        collectSuffixes(tree, mapType, true);
      }
    }

    int loop = 1;
    if (profile) {
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

    if (estLatency) {
      for (int i = 0; i < loop; i++) {
        resultPrinter.latency = MainSupport.estimateLatency(tree, dataSet, mergeStrategy, mapType);
        resultPrinter.recordLatency();
      }
    }

    if (inspect) {
      NodeInspector ni = new NodeInspector();
      ni.inspect(tree.root);
      System.out.println(ni);
      ni.dumpDepthResults(resultPrinter);
    }

    REPORT_CHANNEL.append("FINISH:" + String.join(" ", argList) + " with EF code: " + CDM_WITH_EF);
    REPORT_CHANNEL.append("\n");
    System.out.println(REPORT_CHANNEL);
    System.out.flush();
    InternalInspector.printResult();
  }

  private void resetStaticArgs() {
    REPORT_CHANNEL.delete(0, REPORT_CHANNEL.length());
  }

  public static void main(String[] args) {
    Main m = new Main();
    m.mainbody(args);
  }


  private void setByArgs(String[] _args) {
    List<String> args = new ArrayList<>();
    Set<String> hashedArgs = new HashSet<>();

    for (String s : _args) {
      if (!hashedArgs.contains(s)) {
        args.add(s);
        hashedArgs.add(s);
      } else {
        throw new RuntimeException("args duplicated:" + Arrays.toString(_args));
      }
    }

    int idx = -1;
    if ((idx = args.indexOf("-alias")) >= 0) {
      switch (args.get(idx + 1).toUpperCase()) {
        case "MTREE":
          dataAlias = "MTree";
          mapType = MapType.HASH;
          mergeStrategy = PrefixMergeStrategy.NO_MERGE;
          flatTree = false;
          break;
        case "ART":
          dataAlias = "ART";
          mapType = MapType.FDM;
          mergeStrategy = PrefixMergeStrategy.FULL;
          flatTree = true;
          break;
        case "OLD_CDM":
          dataAlias = "OLD_CDM";
          mapType = MapType.CDM;
          mergeStrategy = PrefixMergeStrategy.FULL;
          flatTree = true;
          break;
        case "NEW_CDM":
          dataAlias = "NEW_CDM";
          mapType = MapType.NCDM;
          mergeStrategy = PrefixMergeStrategy.FULL;
          flatTree = true;
          break;
        default:
          throw new UnsupportedOperationException();
      }
    } else {
      if ((idx = args.indexOf("-mt")) >= 0) {
        mapType = MapType.valueOf(args.get(idx + 1).toUpperCase());
      }

      if ((idx = args.indexOf("-ms")) >= 0) {
        mergeStrategy = PrefixMergeStrategy.valueOf(args.get(idx + 1).toUpperCase());
      } else {
        mergeStrategy = PrefixMergeStrategy.NO_MERGE;
      }

      flatTree = hashedArgs.contains("-oneTree");

      if (mapType == MapType.FDM ^ mergeStrategy == PrefixMergeStrategy.FULL) {
        throw new UnsupportedOperationException("FDM can only FULL MERGE now.");
      }
    }

    if ((idx = args.indexOf("-ds")) != -1) {
      dataSet = MyDataSet.valueOf(args.get(idx + 1).toUpperCase());
    }

    estSpace = hashedArgs.contains("-space");
    estLatency = hashedArgs.contains("-latency");
    profile = hashedArgs.contains("-profile");
    inspect = hashedArgs.contains("-inspect");
    mergeSuffix = hashedArgs.contains("-template");
    logPrint = hashedArgs.contains("-logPrint");
  }
}
