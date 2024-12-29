package optimize;

import loader.PathTxtLoader;
import optimize.merge.MapType;
import optimize.merge.PrefixMergeStrategy;
import org.openjdk.jol.info.GraphLayout;
import org.openjdk.jol.info.GraphStats;
import seart.metric.TreeCompare;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

public class MainSupport {
  public static String getBuildTimestamp() {
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

  public static TSTree buildLogicalTree(MyDataSet ds, boolean twoLevel) {
    TSTree tree = new TSTree();
    tree.setTwoLevelPath(twoLevel);
    try (PathTxtLoader loader = new PathTxtLoader(ds.rfile)) {
      List<String> paths = loader.getAllLines();
      for (String s : paths) {
        tree.insert(s, s.hashCode());
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    Main.REPORT_CHANNEL.append(String.format("Logical tree with %d nodes \n", tree.nodeNum.get()));
    return tree;
  }

  public static long measureSpace(TSTree tree, PrefixMergeStrategy ms, MapType mt, boolean detail) {
    GraphLayout gl = GraphLayout.parseInstance(tree);
    long size = gl.totalSize();
    if (detail) {
      System.out.println(gl.toFootprint());
    }
    Main.REPORT_CHANNEL
        .append(String.format("%s %s total Size: ", ms == null ? "no-merge" : ms.name(), mt.name()))
        .append(size)
        .append("\n");
    return size;
  }

  public static long estimateLatency(
      TSTree tree, MyDataSet ds, PrefixMergeStrategy ms, MapType mt) {
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
        if (ms.equals(PrefixMergeStrategy.NO_MERGE)) {
          nano = System.nanoTime();
          for (int i = 0; i < qPaths.size(); i++) {
            if (ans[i] != tree.searchLogical(qPaths.get(i)))
              throw new RuntimeException("Search for worng!");
          }
          nano = System.nanoTime() - nano;
        } else {
          nano = System.nanoTime();
          for (int i = 0; i < qPaths.size(); i++) {
            if (ans[i] != tree.searchHash(qPaths.get(i)))
              throw new RuntimeException("Search for worng!");
          }
          nano = System.nanoTime() - nano;
        }
        break;
    }

    Main.REPORT_CHANNEL.append(
        String.format(
            "%s %s query %d paths latency(ns): %s ns. \n",
            ms == null ? "no-merge" : ms.name(), mt.name(), qPaths.size(), dottedNanoSec(nano)));
    return nano;
  }

  public static String dottedNanoSec(long nano) {
    return String.format("%d.%06d", nano / 1_000_000, nano % 1_000_000);
  }
}
