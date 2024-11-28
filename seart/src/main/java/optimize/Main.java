package optimize;

import loader.PathTxtLoader;
import optimize.nodes.fdm.vfull.SEARTree;
import optimize.nodes.logic.LNode;
import optimize.nodes.hash.HNode;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.info.GraphLayout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static optimize.Main.DataSet.BW;
import static optimize.nodes.cdm.CNodeHelper.strings2ByteArrays;

public class Main extends MergePrefix {



  public static TSTree buildLogicalTree(DataSet ds) {
    TSTree tree = new TSTree();
    try (PathTxtLoader loader = new PathTxtLoader(PathTxtLoader.FILE_PATH)) {
      List<String> paths = loader.getAllLines();
      for (String s : paths) {
        tree.insert(s, s.hashCode());
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    System.out.println(String.format("Logical tree with %d nodes", tree.nodeNum.get()));
    return tree;
  }

  public static void measureSpace(TSTree tree) {
    long size = GraphLayout.parseInstance(tree).totalSize();
    System.out.println("Total Size:" + size);
  }

  public static void estimateLatency(TSTree tree) {
    final List<String> qPaths = new ArrayList<>();
    try (PathTxtLoader loader = new PathTxtLoader("mtreedata/baowu_query.txt")) {
      qPaths.addAll(loader.getAllLines());
    } catch (Exception e) {
      e.printStackTrace();
    }

    long[] ans = new long[qPaths.size()];
    Arrays.parallelSetAll(ans, i -> qPaths.get(i).hashCode());
    long nano = System.nanoTime();
    for (int i = 0; i < qPaths.size(); i++) {
      if (ans[i] != tree.search(qPaths.get(i))) throw new RuntimeException("Search for worng!");
    }
    nano = System.nanoTime() - nano;

    if (mergeStrategy.equals(Evaluator.MergeStrategy.FULL) && mapType.equals(Evaluator.MapType.FDM)) {
      System.out.println("total not exist key: " + SEARTree.nullKeys);
    }

    System.out.println(String.format("query for %d ns / %d paths", nano, qPaths.size()));
  }

  public static void replaceTemplates(TSTree tree) {

  }

  public static String[] defaultArgs() {
    String res = "";
    res += " -mt hash";
    // res += " -mt fdm";
    // res += " -mt cdm";
    // res += " -ms full";
    // res += " -ms partial";
    res += " -ms simple";
    res += " -ds bw";

    // res += " -merge";
    // res += " -latency";
    res += " -space";

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
    int argIdx = 0;
    if ((argIdx = argList.indexOf("-mt")) != -1) {
      mapType = Evaluator.MapType.valueOf(argList.get(argIdx + 1).toUpperCase());
    }
    if ((argIdx = argList.indexOf("-ms")) != -1) {
      mergeStrategy = Evaluator.MergeStrategy.valueOf(argList.get(argIdx + 1).toUpperCase());
    }
    if ((argIdx = argList.indexOf("-ds")) != -1) {
      dataSet = DataSet.valueOf(argList.get(argIdx + 1).toUpperCase());
    }

    TSTree tree = buildLogicalTree(BW);
    if (argList.contains("-merge")) {
      mergePrefixes(tree, mapType, mergeStrategy);
    } else {
      AtomicInteger atomicInteger = new AtomicInteger(0);
      tree.traversePostOrderRec((par, key, cur, stk) -> {
        if (cur.getKeys() != null) atomicInteger.incrementAndGet();
      });
      System.out.println("Internal Nodes: " + atomicInteger.get());
    }
    replaceTemplates(tree);

    if (argList.contains("-space")) {
      measureSpace(tree);
    }

    if (argList.contains("-latency")) {
      estimateLatency(tree);
    }
    System.out.println("FINISH:" + String.join(" ", argList));
  }

  // test measurement
  public static void mainv(String[] args) {
    HNode sn = new HNode();
    LNode ln = new LNode();
    String a = new String("AAA");
    byte[] b = new byte[] {1,2,3};
    System.out.println(ClassLayout.parseInstance(sn).toPrintable());
    System.out.println(ClassLayout.parseInstance(ln).toPrintable());
    System.out.println(GraphLayout.parseInstance(a).toPrintable());
    System.out.println(ClassLayout.parseInstance(b).toPrintable());
  }

  public enum DataSet {
    BW,
    SW,
    ZY,
    XYZC;
  }
}
