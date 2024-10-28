package seart.utils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import loader.PathTxtLoader;
import seart.SEARTree;

public class PathUtils {
  public static List<String> joinStringLists(List<String> front, List<String> back) {
    Set<String> res = new HashSet<>();
    for (String f : front) {
      for (String b : back) {
        res.add(f + "." + b);
      }
    }
    return new ArrayList<>(res);
  }

  public static void main(String[] args) throws IOException {
    cleanPrefixOverlap("mtreedata/dev.txt", "mtreedata/dev.txt");
  }

  public static void cleanPrefixOverlap(String srcFile, String dstFile) throws IOException {
    List<String> bat, srcPath = new ArrayList<>();
    PathTxtLoader loader = new PathTxtLoader(srcFile);
    while (!(bat = loader.getLines()).isEmpty()) {
      srcPath.addAll(bat);
    }

    loader.close();
    // check to dump devs with prefix property
    SEARTree tree = new SEARTree();
    Set<String> success = new HashSet<>();
    Random random = new Random();
    for (String s : srcPath) {
      int i = 1;
      while (!insertWithReturn(tree, s)) {
        System.out.println("dup: " + s);
        String[] nodes = s.split("\\.");
        nodes[2] = "baoshanan";
        nodes[3] =
            UUID.nameUUIDFromBytes(Long.toString(random.nextLong()).getBytes())
                    .toString()
                    .toUpperCase()
                    .substring(0, 7)
                + "ZX";
        s = String.join(".", nodes);
        System.out.println(String.format("repeat %d times for %s", i, s));
        i++;
      }
      success.add(s);
    }

    System.out.println(String.format("succ: %d, all:%d", success.size(), srcPath.size()));

    dumpStringCollection(dstFile, success);
    System.out.println("finish");
  }

  public static boolean insertWithReturn(SEARTree tree, String k) {
    try {
      tree.insert(k, 0L);
    } catch (Throwable e) {
      // e.printStackTrace();
      return false;
    }

    return true;
  }

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

  public static void dumpStringCollection(String path, Collection<String> col) {
    Path filePath = Paths.get(path);

    try (BufferedWriter writer = Files.newBufferedWriter(filePath)) {
      for (String str : col) {
        writer.write(str);
        writer.newLine();
      }
      System.out.println("Success to write " + path);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static String removeBacktick(String src) {
    return src.replace("`", "");
  }

  public static String[] getNodes(String src) {
    return src.split("\\.");
  }
}
