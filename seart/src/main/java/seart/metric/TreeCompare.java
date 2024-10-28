package seart.metric;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.stream.Collectors;
import loader.PathTxtLoader;
import mtree.MTreeMeasure;
import org.openjdk.jol.info.GraphLayout;
import seart.SEARTree;
import seart.SeriesIndexTree;
import seart.miner.MockSubtreeMiner;
import seart.serlzer.PostOrderSwizzle;
import seart.utils.PathUtils;

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

  private static volatile long nanoSec;
  private static String[][] searchPathArray;

  private static Set<String> modDevs, sens;

  public static SeriesIndexTree[] loadTreeMeasureSpace(DataFile fileSet, byte treeFlag)
      throws Exception {
    PathTxtLoader devLoader = new PathTxtLoader(fileSet.allDevFile);
    Set<String> allDevs = new HashSet<>(devLoader.getAllLines());
    devLoader.close();

    Set<String> nonModDevs = new HashSet<>();
    for (String d : allDevs) {
      if (!modDevs.contains(d)) {
        nonModDevs.add(d);
      }
    }
    List<String> allPaths =
        PathUtils.joinStringLists(new ArrayList<>(allDevs), new ArrayList<>(sens));
    List<String> nonTpltPaths =
        PathUtils.joinStringLists(new ArrayList<>(nonModDevs), new ArrayList<>(sens));

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

    if (measureSpace) {
      System.out.println("Measuring spaces...");
      System.out.println(
          String.format(
              "Space cost: mtree: %d, cart: %d, seart: %d",
              GraphLayout.parseInstance(mtree).totalSize(),
              GraphLayout.parseInstance(artTree).totalSize(),
              GraphLayout.parseInstance(searTree).totalSize()));
    }

    return new SeriesIndexTree[] {mtree, artTree, searTree, tpltTree};
  }

  private static void buildSearchPaths() {
    List<String> uniquePaths = new ArrayList<>();
    for (String d : modDevs) {
      for (String s : sens) {
        uniquePaths.add(d + "." + s);
      }
    }

    searchPathArray = new String[SEARCH_EPOCH][];
    for (int i = 0; i < SEARCH_EPOCH; i++) {
      Collections.shuffle(uniquePaths);
      searchPathArray[i] = new String[uniquePaths.size()];
      for (int j = 0; j < uniquePaths.size(); j++) {
        searchPathArray[i][j] = uniquePaths.get(j);
      }
    }
  }

  private static void initModDevAndSens(DataFile fileSet) throws Exception {
    PathTxtLoader modLoader = new PathTxtLoader(fileSet.modDevFile);
    PathTxtLoader senLoader = new PathTxtLoader(fileSet.senFile);
    modDevs = new HashSet<>(modLoader.getAllLines());
    sens = new HashSet<>(senLoader.getAllLines());
    modLoader.close();
    senLoader.close();
  }

  private static void serializeTrees(SeriesIndexTree[] trees, byte treeFlag) throws IOException {
    byte[] flags = new byte[] {MTREE, CART, SEART};
    for (int i = 0; i < objFileName.length; i++) {
      if ((flags[i] & treeFlag) != 0) {
        nanoSec = System.nanoTime();
        if (flags[i] == CART || flags[i] == SEART) {
          PostOrderSwizzle.serializeSEART(trees[i], objFileName[i]);

        } else {
          // for MTree
          try (ObjectOutputStream oos =
              new ObjectOutputStream(new FileOutputStream(objFileName[i]))) {
            oos.writeObject(trees[i]);
          }
        }
        reportTime("Persist " + objFileName[i], System.nanoTime() - nanoSec);
      }
    }
  }

  public static SeriesIndexTree[] deserializeTrees(byte treeFlag)
      throws IOException, ClassNotFoundException {
    SeriesIndexTree[] res = new SeriesIndexTree[3];
    byte[] flags = new byte[] {MTREE, CART, SEART};
    for (int i = 0; i < objFileName.length; i++) {
      if ((flags[i] & treeFlag) != 0) {
        nanoSec = System.nanoTime();
        if (flags[i] == CART || flags[i] == SEART) {
          SEARTree[] allRes = PostOrderSwizzle.loadSEART(objFileName[i]);
          res[i] = allRes[0];
        } else {
          try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(objFileName[i]))) {
            System.out.println(String.format("Deserializing %s...", objFileName[i]));
            res[i] = (SeriesIndexTree) ois.readObject();
          }
        }
        reportTime("Load " + objFileName[i], System.nanoTime() - nanoSec);
      }
    }
    return res;
  }

  private static String[] defaultArgs() {
    String param = "seart md75_s6";
    param += " search";
    param += " check";
    param += " persist";
    // param += " build";
    // param += " measure";
    return param.split(" ");
  }

  static final byte MTREE = 0x01, CART = 0x02, SEART = 0x04;
  private static int SEARCH_EPOCH = 1;
  private static String[] objFileName = new String[] {"mtree.obj", "cart.obj", "seart.obj"};
  private static boolean toSearch, measureSpace, persist, check;
  // main for jar entrance
  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      args = defaultArgs();
      System.out.println("Using embedded args: " + Arrays.toString(args));
    }
    System.out.println("Build Time:" + getBuildTimestamp());
    Set<String> options =
        Arrays.asList(args).stream().map(String::toLowerCase).collect(Collectors.toSet());
    toSearch = options.contains("search");
    measureSpace = options.contains("measure");
    persist = options.contains("persist");
    check = options.contains("check");

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

    SeriesIndexTree[] indexTrees;
    initModDevAndSens(fileSet);
    if (options.contains("build")) {
      System.out.println("Building and persisting trees...");
      System.out.println(
          String.format(
              "Parameters: %s, %s, %s, %s", fileSet.name(), treeFlag, measureSpace, toSearch));
      indexTrees = loadTreeMeasureSpace(fileSet, treeFlag);
      if (persist) serializeTrees(indexTrees, treeFlag);
    } else {
      System.out.println(String.format("Loading trees for %s.", fileSet.name()));
      indexTrees = deserializeTrees(treeFlag);
    }

    System.out.println(String.format("Building search paths for %d epochs.", SEARCH_EPOCH));
    buildSearchPaths();

    System.out.println("Press any key to continue.");
    int a = System.in.read();

    if (toSearch) {

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

      searchAndPrint(searchNames, searchTrees);
    }
  }

  private static void searchAndPrint(List<String> names, List<SeriesIndexTree> indexTrees) {
    for (int i = 0; i < indexTrees.size(); i++) {
      long[] totalTime = new long[SEARCH_EPOCH];
      for (int j = 0; j < SEARCH_EPOCH; j++) {
        nanoSec = System.nanoTime();
        int size = searchPathArray[j].length;

        for (int k = 0; k < size; k++) {
          long res = indexTrees.get(i).search(searchPathArray[j][k]);
          if (check && res != searchPathArray[j][k].hashCode()) {
            throw new RuntimeException("Search result error.");
          }
        }
        totalTime[j] = (System.nanoTime() - nanoSec) / 1000000;
      }
      System.out.println(
          String.format(
              "Search %s for %s nano-secs per epoch.", names.get(i), Arrays.toString(totalTime)));
    }
    System.gc();
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
    List<String> allPaths =
        PathUtils.joinStringLists(new ArrayList<>(allDevs), new ArrayList<>(sens));
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

    measureSpace = true;
    loadTreeMeasureSpace(DataFile.MD75_S6, (byte) (SEART | CART | MTREE));

    // measure all
  }

  private static void reportTime(String mes, long nano) {
    System.out.println(mes + " for " + nano / 1000000 + " mil-secs.");
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
