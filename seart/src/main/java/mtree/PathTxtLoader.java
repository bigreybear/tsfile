package mtree;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
    int cnt = 0;
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

  public static void main(String[] args) throws IOException {
    PathTxtLoader loader = new PathTxtLoader("mtreedata/text_series.txt");
    List<String> bat;
    while (!(bat = loader.getLines()).isEmpty()) {
      for (String p : bat) {
        System.out.println(PathTxtLoader.removeBacktick(p));
      }
    }
  }
}
