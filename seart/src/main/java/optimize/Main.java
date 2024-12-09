package optimize;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import loader.PathTxtLoader;
import org.openjdk.jol.info.GraphLayout;

import static optimize.SuffixMerge.collectSuffixes;

public class Main extends MergePrefix {

  public static TSTree buildLogicalTree(DataSet ds) {
    TSTree tree = new TSTree();
    try (PathTxtLoader loader = new PathTxtLoader(ds.rfile)) {
      List<String> paths = loader.getAllLines();
      for (String s : paths) {
        tree.insert(s, s.hashCode());
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    REPORT_CHANNEL.append(String.format("Logical tree with %d nodes \n", tree.nodeNum.get()));
    return tree;
  }

  public static void measureSpace(TSTree tree, Evaluator.MergeStrategy ms, Evaluator.MapType mt) {
    long size = GraphLayout.parseInstance(tree).totalSize();
    REPORT_CHANNEL
        .append(String.format("%s %s total Size: ", ms == null ? "no-merge" : ms.name(), mt.name()))
        .append(size)
        .append("\n");
  }

  public static void estimateLatency(TSTree tree, DataSet ds, Evaluator.MergeStrategy ms, Evaluator.MapType mt) {
    final List<String> qPaths = new ArrayList<>();
    try (PathTxtLoader loader = new PathTxtLoader(ds.qfile)) {
      qPaths.addAll(loader.getAllLines());
    } catch (Exception e) {
      e.printStackTrace();
    }

    long[] ans = new long[qPaths.size()];
    Arrays.parallelSetAll(ans, i -> qPaths.get(i).hashCode());

    long nano = 0;

    switch (mt) {
      case FDM:
        nano = System.nanoTime();
        for (int i = 0; i < qPaths.size(); i++) {
          if (ans[i] != tree.searchFDM(qPaths.get(i))) throw new RuntimeException("Search for worng!");
        }
        nano = System.nanoTime() - nano;
        break;
      case CDM:
        nano = System.nanoTime();
        for (int i = 0; i < qPaths.size(); i++) {
          if (ans[i] != tree.searchCDM(qPaths.get(i))) throw new RuntimeException("Search for worng!");
          // if (ans[i] != tree.search(qPaths.get(i))) throw new RuntimeException("Search for worng!");
        }
        nano = System.nanoTime() - nano;
        break;
      case HASH:
        nano = System.nanoTime();
        for (int i = 0; i < qPaths.size(); i++) {
          if (ans[i] != tree.searchHash(qPaths.get(i))) throw new RuntimeException("Search for worng!");
          // if (ans[i] != tree.search(qPaths.get(i))) throw new RuntimeException("Search for worng!");
        }
        nano = System.nanoTime() - nano;
        break;
    }

    REPORT_CHANNEL.append(
        String.format(
            "%s %s query %d paths latency(ns): %s ns. \n",
            ms == null ? "no-merge" : ms.name(),
            mt.name(),
            qPaths.size(),
            dottedNanoSec(nano)));
  }

  private static String dottedNanoSec(long nano) {
    return String.format("%d.%06d", nano / 1_000_000, nano % 1_000_000);
  }

  public static void replaceTemplates(TSTree tree) {}

  // configurations
  public static final boolean CDM_WITH_EF = false;

  public static final StringBuilder REPORT_CHANNEL = new StringBuilder();

  public static String[] defaultArgs() {
    String res = "";
    // res += " -mt hash";
    // res += " -mt fdm";
    res += " -mt cdm";
    // res += " -ms full";
    res += " -ms partial";
    // res += " -ms simple";
    res += " -ds bw";
    // res += " -ds xyzc";
    // res += " -ds sw";
    // res += " -ds zy";

    res += " -merge";
    res += " -latency";
    // res += " -space";
    // res += " -template";

    return res.split(" ");
  }

  // build logical tree
  // chooses map
  public static DataSet dataSet;
  public static Evaluator.MergeStrategy mergeStrategy;
  public static Evaluator.MapType mapType;

  public static void main(String[] args) {
    args = args.length == 0 ? defaultArgs() : args;
    List<String> argList = Arrays.stream(args).distinct().collect(Collectors.toList());
    if (argList.size() != args.length) throw new RuntimeException("duplicated args.");
    int argIdx = 0;
    if ((argIdx = argList.indexOf("-ms")) != -1) {
      mergeStrategy = Evaluator.MergeStrategy.valueOf(argList.get(argIdx + 1).toUpperCase());
    }
    if ((argIdx = argList.indexOf("-ds")) != -1) {
      dataSet = DataSet.valueOf(argList.get(argIdx + 1).toUpperCase());
    }
    if ((argIdx = argList.indexOf("-mt")) != -1) {
      mapType = Evaluator.MapType.valueOf(argList.get(argIdx + 1).toUpperCase());
      if (mapType.equals(Evaluator.MapType.FDM)) {
        mergeStrategy = Evaluator.MergeStrategy.FULL;
      }
    }

    TSTree tree = buildLogicalTree(dataSet);

    if (argList.contains("-merge")) {
      mergePrefixes(tree, mapType, mergeStrategy);
    } else {
      AtomicInteger atomicInteger = new AtomicInteger(0);
      tree.traversePostOrderRec(
          (par, key, cur, stk) -> {
            if (cur.getKeys() != null) atomicInteger.incrementAndGet();
          });
      System.out.println("Internal Nodes: " + atomicInteger.get());
    }

    if (argList.contains("-space") && argList.contains("-template")) {
      REPORT_CHANNEL.append(String.format("Before Traversal: %d \n",
          GraphLayout.parseInstance(tree).totalSize()));
      collectSuffixes(tree, mapType, false);
      REPORT_CHANNEL.append(String.format("Before template space: %d \n",
          GraphLayout.parseInstance(tree).totalSize()));
    }
    // always traverse the tree for fair
    if (argList.contains("-template")) {
      collectSuffixes(tree, mapType, true);
      if (argList.contains("-space")) {
        REPORT_CHANNEL.append(String.format("After template space: %d \n",
            GraphLayout.parseInstance(tree).totalSize()));
      }
    }

    if (argList.contains("-space")) {
      measureSpace(tree, mergeStrategy, mapType);
    }

    if (argList.contains("-latency")) {
      estimateLatency(tree, dataSet, mergeStrategy, mapType);
    }
    REPORT_CHANNEL.append("FINISH:" + String.join(" ", argList) + " with EF code: " + CDM_WITH_EF);
    REPORT_CHANNEL.append("\n\n");
    System.out.println(REPORT_CHANNEL);
  }

  private static String DATASET_DIR = "mtreedata/";
  public enum DataSet {
    BW(DATASET_DIR + "text_series.txt", DATASET_DIR + "baowu_query.txt"),
    SW(DATASET_DIR + "sw/path.txt", DATASET_DIR + "sw/query.txt"),
    ZY(DATASET_DIR + "ZY.txt", DATASET_DIR  + "ZY-query.txt"),
    XYZC(DATASET_DIR + "xyzc/boxmeas_path.txt", DATASET_DIR + "xyzc/boxmeas_query.txt");

    public final String rfile;
    public final String qfile;

    DataSet(String a, String b) {
      rfile = a;
      qfile = b;
    }
  }
}
