package seart.metric;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.stream.Collectors;
import loader.PathTxtLoader;
import mtree.MTreeMeasure;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.info.GraphLayout;
import seart.ISEARTNode;
import seart.RefNode;
import seart.SEARTree;
import seart.SeriesIndexTree;
import seart.miner.MockSubtreeMiner;
import seart.traversal.DFSTraversal;

public class TreeCompare {

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

  public static void buildTarPaths() throws Exception {
    // build dev.txt
    PathTxtLoader devLoader = new PathTxtLoader("mtreedata/dev.txt");
    Set<String> devs = new HashSet<>(devLoader.getAllLines());
    devLoader.close();

    // build sensor.txt
    // PathTxtLoader senLoader = new PathTxtLoader("mtreedata/sen.txt");
    // Set<String> sens = new HashSet<>(senLoader.getAllLines());
    // senLoader.close();

    PathTxtLoader.dumpStringCollection("mtreedata/md75.txt", extractCollectionByRatio(devs, 0.75f));
  }

  // main to build files
  // public static void main(String[] args) throws Exception{
  //   buildTarPaths();
  // }

  private static <T> List<T> extractCollectionByRatio(Collection<T> src, float ratio) {
    List<T> srcList = new ArrayList<>(src);
    Set<T> dstSet = new HashSet<>();
    final int threshold = (int) (ratio * src.size()), srcNum = src.size();
    Random random = new Random();
    for (int i = 0; ; i++) {
      if (dstSet.size() >= threshold) break;
      if (random.nextFloat() < ratio) {
        dstSet.add(srcList.get(i % srcNum));
      }
    }
    return new ArrayList<>(dstSet);
  }

  public static SeriesIndexTree[] compareOnDataFile(
      DataFile fileSet, byte treeFlag, boolean toMeasureSpace) throws Exception {
    return compareOnDataFile(fileSet, treeFlag, toMeasureSpace, true);
  }

  private static volatile long nanoSec;
  private static List<String> randomizedSearchPaths = new ArrayList<>();

  public static SeriesIndexTree[] compareOnDataFile(
      DataFile fileSet, byte treeFlag, boolean toMeasureSpace, boolean prepareSearch)
      throws Exception {
    PathTxtLoader devLoader = new PathTxtLoader(fileSet.allDevFile);
    PathTxtLoader modLoader = new PathTxtLoader(fileSet.modDevFile);
    PathTxtLoader senLoader = new PathTxtLoader(fileSet.senFile);
    Set<String> allDevs = new HashSet<>(devLoader.getAllLines());
    Set<String> modDevs = new HashSet<>(modLoader.getAllLines());
    Set<String> sens = new HashSet<>(senLoader.getAllLines());
    devLoader.close();
    modLoader.close();
    senLoader.close();

    Set<String> nonModDevs = new HashSet<>();
    for (String d : allDevs) {
      if (!modDevs.contains(d)) {
        nonModDevs.add(d);
      }
    }
    List<String> allPaths = joinStringLists(new ArrayList<>(allDevs), new ArrayList<>(sens));
    List<String> nonTpltPaths = joinStringLists(new ArrayList<>(nonModDevs), new ArrayList<>(sens));

    // insert all paths on mtree and art
    SEARTree artTree = new SEARTree(), searTree = new SEARTree(), tpltTree = new SEARTree();
    MTreeMeasure mtree = new MTreeMeasure();
    int tpltNum = 0;
    Set<String> replaced = new HashSet<>();

    if ((treeFlag & MTREE) != 0) {
      nanoSec = System.nanoTime();
      for (String p : allPaths) {
        mtree.insert(p, p.hashCode());
      }
      System.out.println(
          String.format(
              "Building MTree for %d mil-secs.", (System.nanoTime() - nanoSec) / 1000000));
    }

    if ((treeFlag & CART) != 0) {
      nanoSec = System.nanoTime();
      for (String p : allPaths) {
        artTree.insert(p, p.hashCode());
      }
      System.out.println(
          String.format(
              "Building ARTree for %d mil-secs.", (System.nanoTime() - nanoSec) / 1000000));
    }

    if ((treeFlag & SEART) != 0) {
      nanoSec = System.nanoTime();
      for (String p : nonTpltPaths) {
        searTree.insert(p, p.hashCode());
      }
      for (String td : modDevs) {
        searTree.insert(td, td.hashCode());
      }
      for (String s : sens) {
        tpltTree.insert(s, tpltNum++);
      }
      MockSubtreeMiner.replaceV1(
          searTree.root,
          tpltTree.root,
          (a, b) -> {
            String p = new String(b, StandardCharsets.UTF_8);
            if (modDevs.contains(p)) {
              replaced.add(p);
              return true;
            } else {
              return false;
            }
          });
      System.out.println(
          String.format(
              "Building SEART for %d mil-secs.", (System.nanoTime() - nanoSec) / 1000000));
    }

    System.out.println(
        String.format(
            "Report all-dev: %d, device-on-tplt: %d, sen: %d",
            allDevs.size(), replaced.size(), sens.size()));

    if (toMeasureSpace) {
      System.out.println("Measuring spaces...");
      System.out.println(
          String.format(
              "Space cost: mtree: %d, cart: %d, seart: %d",
              GraphLayout.parseInstance(mtree).totalSize(),
              GraphLayout.parseInstance(artTree).totalSize(),
              GraphLayout.parseInstance(searTree).totalSize()));
    }

    if (prepareSearch) {
      System.out.println("Preparing search paths...");
      // search only on templated paths
      String[] tarPathArray = new String[modDevs.size() * sens.size()];
      int pNum = 0;
      for (String d : modDevs) {
        for (String s : sens) {
          tarPathArray[pNum++] = d + "." + s;
        }
      }

      randomizedSearchPaths.clear();
      for (int i = 0; i < tarPathArray.length; i++) {
        if ((tarPathArray[i].hashCode() & 0xff) > 64) {
          randomizedSearchPaths.add(tarPathArray[i]);
        }
      }
    }

    return new SeriesIndexTree[] {mtree, artTree, searTree, tpltTree};
  }

  // main for jar entrance
  public static void main(String[] args) throws Exception {
    System.out.println("Build Time:" + getBuildTimestamp());
    Set<String> options =
        Arrays.asList(args).stream().map(String::toLowerCase).collect(Collectors.toSet());
    boolean toSearch = options.contains("tos");
    boolean measureSpace = options.contains("mes");

    byte treeFlag = 0x00;
    String[] trees = new String[] {"mtree", "cart", "seart"};
    for (int i = 0; i < trees.length; i++) {
      if (options.contains(trees[i])) treeFlag |= (byte) (0x01 << i);
    }

    DataFile fileSet = null;
    Set<String> dataFileSets =
        Arrays.stream(DataFile.values())
            .map(DataFile::toString)
            .map(String::toLowerCase)
            .collect(Collectors.toSet());
    for (String ops : options) {
      if (dataFileSets.contains(ops)) {
        if (fileSet == null) fileSet = DataFile.valueOf(ops.toUpperCase());
        else throw new RuntimeException("Multiple file sets.");
      }
    }

    System.out.println(
        String.format(
            "Parameters: %s, %s, %s, %s", fileSet.name(), treeFlag, measureSpace, toSearch));
    SeriesIndexTree[] indexTrees = compareOnDataFile(fileSet, treeFlag, measureSpace, toSearch);

    List<SeriesIndexTree> searchTrees = new ArrayList<>();
    List<String> searchNames = new ArrayList<>();
    if ((treeFlag & MTREE) != 0) {
      searchNames.add("mtree");
      searchTrees.add(indexTrees[0]);
    }
    if ((treeFlag & CART) != 0) {
      searchNames.add("cart");
      searchTrees.add(indexTrees[1]);
    }
    if ((treeFlag & SEART) != 0) {
      searchNames.add("seart");
      searchTrees.add(indexTrees[2]);
    }

    if (toSearch) {
      searchAndPrint(searchNames, searchTrees);
    }
  }

  // to test parameter parser
  public static void mainx(String[] args) throws Exception {
    // main2(new String[] {"MD25_S6", "tos", "CART", "MTREE", "SEART", "MD50_S6"});
  }

  // measure building latency
  public static void mainLatency(String[] args) throws Exception {
    System.out.println("8");
    compareOnDataFile(DataFile.MD75_S8, (byte) (CART), NO_MEASURE_SPACE, false);
    compareOnDataFile(DataFile.MD75_S8, (byte) (CART), NO_MEASURE_SPACE, false);
    compareOnDataFile(DataFile.MD75_S8, (byte) (MTREE), NO_MEASURE_SPACE, false);
    compareOnDataFile(DataFile.MD75_S8, (byte) (MTREE), NO_MEASURE_SPACE, false);

    System.out.println("6");
    compareOnDataFile(DataFile.MD75_S6, (byte) (CART), NO_MEASURE_SPACE, false);
    compareOnDataFile(DataFile.MD75_S6, (byte) (CART), NO_MEASURE_SPACE, false);
    compareOnDataFile(DataFile.MD75_S6, (byte) (MTREE), NO_MEASURE_SPACE, false);
    compareOnDataFile(DataFile.MD75_S6, (byte) (MTREE), NO_MEASURE_SPACE, false);

    System.out.println("4");
    compareOnDataFile(DataFile.MD75_S4, (byte) (CART), NO_MEASURE_SPACE, false);
    compareOnDataFile(DataFile.MD75_S4, (byte) (CART), NO_MEASURE_SPACE, false);
    compareOnDataFile(DataFile.MD75_S4, (byte) (MTREE), NO_MEASURE_SPACE, false);
    compareOnDataFile(DataFile.MD75_S4, (byte) (MTREE), NO_MEASURE_SPACE, false);

    System.out.println("2");
    compareOnDataFile(DataFile.MD75_S2, (byte) (CART), NO_MEASURE_SPACE, false);
    compareOnDataFile(DataFile.MD75_S2, (byte) (CART), NO_MEASURE_SPACE, false);
    compareOnDataFile(DataFile.MD75_S2, (byte) (MTREE), NO_MEASURE_SPACE, false);
    compareOnDataFile(DataFile.MD75_S2, (byte) (MTREE), NO_MEASURE_SPACE, false);
  }

  // measure all bytes original size
  public static void mainOnNaiveStructures(String[] args) throws Exception {
    PathTxtLoader devLoader = new PathTxtLoader(DataFile.MD75_S6.allDevFile);
    PathTxtLoader modLoader = new PathTxtLoader(DataFile.MD75_S6.modDevFile);
    PathTxtLoader senLoader = new PathTxtLoader(DataFile.MD75_S6.senFile);
    Set<String> allDevs = new HashSet<>(devLoader.getAllLines());
    Set<String> modDevs = new HashSet<>(modLoader.getAllLines());
    Set<String> sens = new HashSet<>(senLoader.getAllLines());
    devLoader.close();
    modLoader.close();
    senLoader.close();

    long len = 0;
    List<String> allPaths = joinStringLists(new ArrayList<>(allDevs), new ArrayList<>(sens));
    Map<String, Long> rbTree = new TreeMap<>();
    Map<String, Long> hashMap = new HashMap<>();
    for (String p : allPaths) {
      len += p.getBytes(StandardCharsets.UTF_8).length + 8;
      rbTree.put(p, (long) p.hashCode());
      hashMap.put(p, (long) p.hashCode());
    }

    System.out.println(allPaths.size());
    System.out.println(len);
    System.out.println(GraphLayout.parseInstance(rbTree).totalSize());
    System.out.println(GraphLayout.parseInstance(hashMap).totalSize());

    compareOnDataFile(DataFile.MD75_S6, (byte) (SEART | CART | MTREE), MEASURE_SPACE);

    // measure all
  }

  static final byte MTREE = 0x01, CART = 0x02, SEART = 0x04;
  static final boolean MEASURE_SPACE = true, NO_MEASURE_SPACE = false;
  // complete test
  public static void main_2(String[] args) throws Exception {
    SeriesIndexTree[] indexTrees;
    String[] names = new String[] {"mtree", "cart", "seart"};

    indexTrees = compareOnDataFile(DataFile.MD25_S2, (byte) (SEART | CART), MEASURE_SPACE);
    indexTrees = compareOnDataFile(DataFile.MD50_S2, (byte) (SEART | CART), MEASURE_SPACE);
    indexTrees = compareOnDataFile(DataFile.MD75_S2, (byte) (SEART | CART), MEASURE_SPACE);
    indexTrees = compareOnDataFile(DataFile.MD100_S2, (byte) (SEART | CART), MEASURE_SPACE);

    System.out.println("HEAD");
  }

  // for minor exps
  public static void mainExp(String[] args) throws Exception {
    // SeriesIndexTree[] indexTrees = compareOnDataFile(DataFile.MD75_S2, NO_MEASURE_SPACE, CART);
    // SeriesIndexTree[] indexTrees2 = compareOnDataFile(DataFile.MD75_S2, NO_MEASURE_SPACE, CART);
    // SeriesIndexTree cart1 = indexTrees[1], cart2 = indexTrees2[1];
    // SEARTree tplTree = (SEARTree) indexTrees[3];

    String[] sensors = new String[] {"I.hz", "frequency"};
    String[] devs =
        new String[] {
          "root.bw.baoshan.218914I19.00.CCM2_NO4TUNDISHC_AR_BAGCOVER_AR_PRSS",
          "root.bw.baoshan.820622M03.00.RADIATION_PIPE_IGNITION_CONTROL_STATUS_AJ_BCM7_REST_OUT",
          "root.bw.baoshan.828905I06-1.01.备用电池电量",
          "root.bw.baoshan.010101M11.00.A101BC_BACK_CHANGE_DIRECTION_ROLLER_B_SIDE_V_SUPER_TOTAL_VALUE_SPEED",
          "root.bw.baoshan.682552M03.33.16k加速度波形(2-20000)",
          "root.bw.baoshan.021408M03.03.16k加速度波形(2-2000)",
          "root.bw.baoshan.711270E05.01.低频加速度RMS",
          "root.bw.baoshan.315823E01.03.接地跳闸"
        };

    List<String> allPaths = joinStringLists(Arrays.asList(devs), Arrays.asList(sensors));
    SEARTree cartNoTemplate = buildTrees(allPaths.toArray(new String[0]));

    SEARTree template = buildTrees(sensors);
    List<String> allButNotFirst =
        joinStringLists(Arrays.asList(devs).subList(1, devs.length), Arrays.asList(sensors));
    SEARTree repTree = buildTrees(allButNotFirst.toArray(new String[0]));
    repTree.insert(devs[0], 0L);
    MockSubtreeMiner.replaceV1(
        repTree.root,
        template.root,
        (a, b) -> new String(b, StandardCharsets.UTF_8).equals(devs[0]));

    long size1 = GraphLayout.parseInstance(repTree).totalSize();
    long size2 = GraphLayout.parseInstance(cartNoTemplate).totalSize();

    System.out.println("---");
    DFSTraversal.printAllPaths2(repTree);
    System.out.println("---");
    DFSTraversal.printAllPaths2(template);
    System.out.println("---");
    DFSTraversal.printAllPaths2(cartNoTemplate);
    System.out.println("---");

    List<ISEARTNode> path1 =
        SEARTree.getPrefixPaths(
            repTree.root,
            "root.bw.baoshan.218914I19.00.CCM2_NO4TUNDISHC_AR_B".getBytes(StandardCharsets.UTF_8),
            0,
            null);
    List<ISEARTNode> path2 =
        SEARTree.getPrefixPaths(
            cartNoTemplate.root,
            "root.bw.baoshan.218914I19.00.CCM2_NO4TUNDISHC_AR_B".getBytes(StandardCharsets.UTF_8),
            0,
            null);

    System.out.println(GraphLayout.parseInstance(path1.get(0).getChildByPtrIndex(1)).toFootprint());
    System.out.println(GraphLayout.parseInstance(path2.get(0).getChildByPtrIndex(1)).toFootprint());
    System.out.println(String.format("%d, %d", size1, size2));
    System.out.println("FINISH");

    System.out.println(
        GraphLayout.parseInstance(((RefNode) path1.get(0).getChildByPtrIndex(1)).templateRoot)
            .toFootprint());
    System.out.println(GraphLayout.parseInstance(path2.get(0).getChildByPtrIndex(1)).toFootprint());

    System.out.println("----");

    System.out.println(
        GraphLayout.parseInstance(path2.get(0).getChildByPtrIndex(1).getChildByPtrIndex(0))
            .toFootprint());
    System.out.println(
        ClassLayout.parseInstance(path2.get(0).getChildByPtrIndex(1).getChildByPtrIndex(0))
            .toPrintable());
  }

  private static SEARTree buildTrees(String... paths) {
    SEARTree tree = new SEARTree();
    for (String p : paths) {
      tree.insert(p, p.hashCode());
    }
    return tree;
  }

  private static List<String> joinStringLists(List<String> front, List<String> back) {
    List<String> res = new ArrayList<>();
    for (String f : front) {
      for (String b : back) {
        res.add(f + "." + b);
      }
    }
    return res;
  }

  private static int searchOnce(SeriesIndexTree tree, String name) {
    Collections.shuffle(randomizedSearchPaths);
    nanoSec = System.nanoTime();
    int cnt = randomizedSearchPaths.size();
    for (int i = 0; i < cnt; i++) {
      tree.search(randomizedSearchPaths.get(i));
    }
    long res = System.nanoTime() - nanoSec;

    return (int) (res / cnt);
  }

  // search on each tree 5 times and print all results
  private static void searchAndPrint(List<String> names, List<SeriesIndexTree> indexTrees) {
    for (int i = 0; i < indexTrees.size(); i++) {
      long[] totalTime = new long[5];
      for (int j = 0; j < 5; j++) {
        totalTime[j] = searchOnce(indexTrees.get(i), names.get(i));
      }
      System.out.println(
          String.format(
              "Search %s for %s nano-secs per path", names.get(i), Arrays.toString(totalTime)));
    }
    System.gc();
  }

  private enum DataFile {
    MD25_S6("md25.txt", "sensor6.txt"),
    MD50_S6("md50.txt", "sensor6.txt"),
    MD75_S6("md75.txt", "sensor6.txt"),
    MD100_S6("dev.txt", "sensor6.txt"),
    MD75_S2("md75.txt", "sensor2.txt"),
    MD25_S2("md25.txt", "sensor2.txt"),
    MD50_S2("md50.txt", "sensor2.txt"),
    MD100_S2("dev.txt", "sensor2.txt"),
    MD75_S4("md75.txt", "sensor4.txt"),
    MD75_S8("md75.txt", "sensor8.txt");

    static final String dir = "mtreedata/";
    final String allDevFile = dir + "dev.txt";
    String modDevFile;
    String senFile;

    DataFile(String mdf, String sf) {
      modDevFile = dir + mdf;
      senFile = dir + sf;
    }
  }
}
