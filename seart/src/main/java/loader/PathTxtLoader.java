package loader;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
    while ((nextLine = reader.readLine()) != null && res.size() < size) {
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
      // 通过流逐行写入
      for (String str : col) {
        writer.write(str);
        writer.newLine();
      }
      System.out.println("Success to write " + path);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) throws IOException {
    PathTxtLoader loader = new PathTxtLoader("mtreedata/text_series.txt");
    List<String> bat;
    Set<String> dev = new HashSet<>(), sen = new HashSet<>();
    while (!(bat = loader.getLines()).isEmpty()) {
      for (String p : bat) {
        // System.out.println(PathTxtLoader.removeBacktick(p));

        String[] nodes = removeBacktick(p).split("\\.");
        int span = (nodes[nodes.length - 2].equals("I") || nodes[nodes.length - 2].equals("S")) ? 2 : 1;
        dev.add(String.join(".", Arrays.copyOfRange(nodes, 0, nodes.length - span)));
        sen.add(String.join(".", Arrays.copyOfRange(nodes, nodes.length - span, nodes.length)));
      }
    }
    dumpStringCollection("mtreedata/dev.txt", dev);
    dumpStringCollection("mtreedata/sen.txt", sen);
    System.out.println("finish");
  }
}
