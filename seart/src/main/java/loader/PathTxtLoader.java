package loader;

import seart.SEARTree;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

public class PathTxtLoader {

  public static String FILE_PATH = "mtreedata/text_series.txt";

  private final BufferedReader reader;

  public PathTxtLoader(String file) throws FileNotFoundException {
    this.reader = new BufferedReader(new FileReader(file));
  }

  public List<String> getLines() throws IOException {
    return getLines(100);
  }

  public List<String> getLines(int size) throws IOException {
    List<String> res = new ArrayList<>();
    String nextLine;
    while (res.size() < size && (nextLine = reader.readLine()) != null) {
      res.add(nextLine);
    }
    return res;
  }

  public void close() throws IOException {
    reader.close();
  }

  public static String removeBacktick(String src) {
    return src. replace("`", "");
  }

  public static String[] getNodes(String src) {
    return src.split("\\.");
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

  public List<String> getAllLines() throws Exception {
    List<String> res = new ArrayList<>();
    String nextLine;
    while ((nextLine = reader.readLine()) != null) {
      res.add(nextLine);
    }
    return res;
  }

  public static void main(String[] args) throws IOException {

    List<String> bat;
    List<String> dev = new ArrayList<>(), sen = new ArrayList<>();


    // PathTxtLoader loader = new PathTxtLoader("mtreedata/text_series.txt");
    // while (!(bat = loader.getLines()).isEmpty()) {
    //   for (String p : bat) {
    //     // System.out.println(PathTxtLoader.removeBacktick(p));
    //
    //     String[] nodes = removeBacktick(p).split("\\.");
    //     int span = (nodes[nodes.length - 2].equals("I") || nodes[nodes.length - 2].equals("S")) ? 2 : 1;
    //     dev.add(String.join(".", Arrays.copyOfRange(nodes, 0, nodes.length - span)));
    //     sen.add(String.join(".", Arrays.copyOfRange(nodes, nodes.length - span, nodes.length)));
    //   }
    // }


    PathTxtLoader loader = new PathTxtLoader("mtreedata/dev.txt");
    while (!(bat = loader.getLines()).isEmpty()) {
      dev.addAll(bat);
    }


    loader.close();
    // check to dump devs with prefix property
    SEARTree tree = new SEARTree();
    Set<String> success = new HashSet<>();
    Random random = new Random();
    for (String s : dev) {
      int i = 1;
      while (!insertWithReturn(tree, s)) {
        System.out.println("dup: " + s);
        String[] nodes = s.split("\\.");
        nodes[2] = "baoshanan";
        nodes[3] = UUID.nameUUIDFromBytes(Long.toString(random.nextLong()).getBytes()).toString().substring(0, 7);
        s = String.join(".", nodes);
        System.out.println(String.format("repeat %d times for %s", i, s));
        i++;
      }
      success.add(s);
    }

    System.out.println(String.format("succ: %d, all:%d", success.size(), dev.size()));

    dumpStringCollection("mtreedata/dev.txt", success);
    // dumpStringCollection("mtreedata/sen.txt", sen);
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
}
