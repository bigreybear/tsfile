package optimize;

import static optimize.merge.SuffixMergeVDev.collectSuffixes;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.stream.Collectors;
import loader.PathTxtLoader;
import optimize.merge.MapType;
import optimize.merge.MergePrefixVDev;
import optimize.merge.PrefixMergeStrategy;
import optimize.traversal.BoxPlotRecord;
import optimize.traversal.MergedTreeTraversalForDepthVDev;
import org.openjdk.jol.info.GraphLayout;
import seart.metric.TreeCompare;

public class Main {

  private static String getBuildTimestamp() {
    try {
      InputStream manifestStream = TreeCompare.class.getResourceAsStream("/META-INF/MANIFEST.MF");
      if (manifestStream != null) {
        Manifest manifest = new Manifest(manifestStream);
        Attributes attributes = manifest.getMainAttributes();
        return attributes.getValue("Build-Timestamp");
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return "Unknown";
  }

  public static TSTreeVDev buildLogicalTree(MyDataSet ds, boolean twoLevel) {
    TSTreeVDev tree = new TSTreeVDev();
    tree.setTwoLevelPath(twoLevel);
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

  public static long measureSpace(TSTreeVDev tree, PrefixMergeStrategy ms, MapType mt) {
    long size = GraphLayout.parseInstance(tree).totalSize();
    REPORT_CHANNEL
        .append(String.format("%s %s total Size: ", ms == null ? "no-merge" : ms.name(), mt.name()))
        .append(size)
        .append("\n");
    return size;
  }

  public static long estimateLatency(
      TSTreeVDev tree, MyDataSet ds, PrefixMergeStrategy ms, MapType mt) {
    final List<String> qPaths = new ArrayList<>();
    try (PathTxtLoader loader = new PathTxtLoader(ds.qfile)) {
      qPaths.addAll(loader.getAllLines());
    } catch (Exception e) {
      e.printStackTrace();
    }

    // byte[][] pathBytes = new byte[qPaths.size()][];
    // Arrays.parallelSetAll(pathBytes, i -> qPaths.get(i).getBytes(StandardCharsets.UTF_8));

    long[] ans = new long[qPaths.size()];
    Arrays.parallelSetAll(ans, i -> qPaths.get(i).hashCode());

    long nano = 0;

    switch (mt) {
      case FDM:
        nano = System.nanoTime();
        for (int i = 0; i < qPaths.size(); i++) {
          if (ans[i] != tree.searchFDM(qPaths.get(i)))
            throw new RuntimeException("Search for worng!");
          // if (ans[i] != tree.searchFDM(pathBytes[i])) throw new RuntimeException("Search for
          // worng!");
        }
        nano = System.nanoTime() - nano;
        break;
      case CDM:
        nano = System.nanoTime();
        for (int i = 0; i < qPaths.size(); i++) {
          // if (ans[i] != tree.searchCDM(pathBytes[i])) throw new RuntimeException("Search for
          // worng!");
          if (ans[i] != tree.searchCDM(qPaths.get(i)))
            throw new RuntimeException("Search for worng!");
        }
        nano = System.nanoTime() - nano;
        break;
      case HASH:
        nano = System.nanoTime();
        for (int i = 0; i < qPaths.size(); i++) {
          // if (ans[i] != tree.searchHash(pathBytes[i])) throw new RuntimeException("Search for
          // worng!");
          if (ans[i] != tree.searchHash(qPaths.get(i)))
            throw new RuntimeException("Search for worng!");
        }
        nano = System.nanoTime() - nano;
        break;
    }

    REPORT_CHANNEL.append(
        String.format(
            "%s %s query %d paths latency(ns): %s ns. \n",
            ms == null ? "no-merge" : ms.name(), mt.name(), qPaths.size(), dottedNanoSec(nano)));
    return nano;
  }

  public static String dottedNanoSec(long nano) {
    return String.format("%d.%06d", nano / 1_000_000, nano % 1_000_000);
  }

  public static void replaceTemplates(TSTreeVDev tree) {}

  // configurations
  public static final boolean CDM_WITH_EF = false;

  public static final StringBuilder REPORT_CHANNEL = new StringBuilder();

  public static String[] defaultArgs() {
    String res = "";
    res += " -mt hash";
    // res += " -mt fdm";
    // res += " -mt cdm";

    res += " -ms full";
    // res += " -ms partial";
    // res += " -ms simple";

    // res += " -ds bw";
    res += " -ds xyzc";
    // res += " -ds sw";
    // res += " -ds zy";

    // res += " -twoLevel";
    res += " -merge";
    res += " -latency";
    // res += " -space";
    // res += " -depth";
    // res += " -template";

    return res.split(" ");
  }

  public static MyDataSet dataSet;
  public static PrefixMergeStrategy mergeStrategy;
  public static MapType mapType;

  public static void main(String[] args) {
    ExpResult result = new ExpResult();
    System.out.println(getBuildTimestamp());
    args = args.length == 0 ? defaultArgs() : args;
    List<String> argList = Arrays.stream(args).distinct().collect(Collectors.toList());
    if (argList.size() != args.length) throw new RuntimeException("duplicated args.");
    int argIdx = 0;
    if ((argIdx = argList.indexOf("-ms")) != -1) {
      mergeStrategy = PrefixMergeStrategy.valueOf(argList.get(argIdx + 1).toUpperCase());
      result.pms = mergeStrategy;
    }
    if ((argIdx = argList.indexOf("-ds")) != -1) {
      dataSet = MyDataSet.valueOf(argList.get(argIdx + 1).toUpperCase());
      result.mds = dataSet;
    }
    if ((argIdx = argList.indexOf("-mt")) != -1) {
      mapType = MapType.valueOf(argList.get(argIdx + 1).toUpperCase());
      result.mapType = mapType;
      if (mapType.equals(MapType.FDM)) {
        mergeStrategy = PrefixMergeStrategy.FULL;
        result.pms = mergeStrategy;
      }
    }

    TSTreeVDev tree = buildLogicalTree(dataSet, argList.contains("-twoLevel"));
    result.twoLevel = argList.contains("-twoLevel");

    if (argList.contains("-merge")) {
      MergePrefixVDev.mergePrefixes(tree, mapType, mergeStrategy);
    }

    BoxPlotRecord res = null;
    if (argList.contains("-depth")) {
      res = MergedTreeTraversalForDepthVDev.collectDepths(tree.root, mapType);
    }

    if (argList.contains("-space") && argList.contains("-template")) {
      // REPORT_CHANNEL.append(String.format("Before Traversal: %d \n",
      //     GraphLayout.parseInstance(tree).totalSize()));
      collectSuffixes(tree, mapType, false);
      REPORT_CHANNEL.append(
          String.format(
              "Before template space: %d \n", GraphLayout.parseInstance(tree).totalSize()));
    }
    // always traverse the tree for fair
    if (argList.contains("-template")) {
      collectSuffixes(tree, mapType, true);
      if (argList.contains("-space")) {
        REPORT_CHANNEL.append(
            String.format(
                "After template space: %d \n", GraphLayout.parseInstance(tree).totalSize()));
      }
    }

    if (argList.contains("-space")) {
      long r = measureSpace(tree, mergeStrategy, mapType);
      result.space = r;
      result.recordSpace();
    }

    if (argList.contains("-latency")) {
      long l = estimateLatency(tree, dataSet, mergeStrategy, mapType);
      result.latency = l;
      result.recordLatency();
    }
    REPORT_CHANNEL.append("FINISH:" + String.join(" ", argList) + " with EF code: " + CDM_WITH_EF);
    REPORT_CHANNEL.append("\n\n");
    System.out.println(REPORT_CHANNEL);
  }
}
